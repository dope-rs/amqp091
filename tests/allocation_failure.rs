use crate::common::recover;
pub mod common;
use common::check::Check;
use dope_amqp091::message::field::view;
use dope_amqp091::message::property::Properties;
use dope_amqp091::message::{Builder, field};
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn config_build_is_fallible() {
    use dope_amqp091::config::Config;
    use dope_amqp091::failure::Error;
    use dope_amqp091::failure::wire::Allocation;

    let (_, attempts) = Tracker::count(|| drop(Config::new("guest", "guest").expect("config")));
    let mut auth = false;
    for index in 0..attempts {
        let (result, activity) =
            Tracker::activity(|| Tracker::fail_after(index, || Config::new("guest", "guest")));
        auth |= matches!(
            result,
            Err(Error::Allocation {
                target: Allocation::Authentication,
                error: o3::alloc::Error::Exhausted,
            })
        );
        assert!(
            matches!(
                result,
                Err(Error::Allocation {
                    error: o3::alloc::Error::Exhausted,
                    ..
                } | Error::Exhausted { .. }
                    | Error::Storage {
                        error: o3::buffer::storage::AllocError::Exhausted(_),
                        ..
                    })
            ),
            "config allocation {index} was not propagated"
        );
        assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
    }
    assert!(auth, "authentication allocation was not exercised");
}

#[test]
fn retains_config() {
    use dope_amqp091::config::Config;
    use dope_amqp091::failure::Error;
    use dope_amqp091::failure::wire::{Allocation, Encode, Encoding};
    use o3::{alloc::Exhausted, buffer::storage};

    let mut counted = Config::new("guest", "guest").expect("config");
    let value = b"value".to_vec();
    let (result, attempts) = Tracker::count(|| counted.try_property("custom", value));
    result.expect("count property insertion");
    drop(counted);
    let last = attempts.checked_sub(1).expect("new property owns a key");

    let (_, activity) = Tracker::activity(|| {
        let mut config = Config::new("guest", "guest").expect("config");
        let value = b"value".to_vec();
        let failed = Tracker::fail_after(last, || config.try_property("custom", value));
        assert!(matches!(
            failed,
            Err(Error::Storage {
                target: Allocation::Short,
                error: storage::AllocError::Exhausted(Exhausted),
            })
        ));
        config
            .try_property("custom", b"retry".to_vec())
            .expect("property retry retains config");

        let rejected = "x".repeat(128);
        assert!(matches!(
            config.try_vhost(&rejected),
            Err(Error::Encoding(Encode::Length {
                kind: Encoding::VirtualHost,
                actual: 128,
                max: 127,
            }))
        ));
        config.try_vhost("/valid").expect("vhost validation retry");
        let failed = Tracker::fail_after(0, || config.try_vhost("/allocation"));
        assert!(matches!(
            failed,
            Err(Error::Storage {
                target: Allocation::VirtualHost,
                error: storage::AllocError::Exhausted(Exhausted),
            })
        ));
        config.try_vhost("/retry").expect("vhost allocation retry");

        let config = config
            .property("custom", b"legacy".to_vec())
            .expect("consuming property builder");
        drop(config.vhost("/legacy").expect("consuming vhost builder"));
    });
    assert_eq!(activity.allocations, activity.deallocations);
    assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
}

#[test]
fn port_build_is_atomic() {
    let ((result, attempts), state) =
        Check(None).build(recover, |result| result.expect("port build fixture"));
    assert_eq!(result.expect("counting build"), 1);
    assert!(attempts > 0, "port build must exercise the allocator");
    assert_eq!(
        state.allocations, state.deallocations,
        "completed port storage must release"
    );
    assert_eq!(state.allocated_bytes, state.deallocated_bytes);

    for index in 0..attempts {
        let ((result, _), state) =
            Check(Some(index)).build(recover, |result| result.expect("port allocation fixture"));
        assert!(result.is_err(), "allocation {index} was not propagated");
        assert_eq!(
            state.allocated_bytes, state.deallocated_bytes,
            "allocation {index} leaked partially built storage"
        );
    }
}

#[test]
fn text_storage_is_bounded() {
    use dope_amqp091::failure::wire::Allocation;
    use o3::alloc;

    let empty = Properties::default();
    assert!(empty.content_type().is_none());

    for initial in [None, Some("x")] {
        let (failed, activity) = Tracker::activity(|| {
            let mut builder = Builder::default();
            if let Some(value) = initial {
                builder = builder.content_type(value).expect("initial content type");
            }
            Tracker::fail_after(0, || builder.content_type("application/json")).err()
        });
        assert!(
            matches!(
                failed,
                Some(dope_amqp091::failure::Error::Allocation {
                    target: Allocation::Property,
                    error: alloc::Error::Exhausted,
                })
            ),
            "property allocation must fail after {initial:?}"
        );
        assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
    }
}

#[test]
fn table_update_is_atomic() {
    let mut table = field::Table::new();
    let failed = Tracker::fail_after(0, || table.insert("", field::Value::from(1i32))).is_err();
    let intact = {
        let table = (field::Value::table(table)).expect("valid table length");
        let table = match table.view() {
            view::Value::Table(table) => Some(table),
            _ => None,
        }
        .expect("expected table value");
        table.is_empty()
    };
    assert!(failed, "field-table allocation failure was not propagated");
    assert!(intact, "failed insertion mutated the field table");

    let mut table = field::Table::new();
    assert!(
        table.insert("key", field::Value::from(1i32)).is_ok(),
        "initial field"
    );
    let replaced = Tracker::fail_after(0, || table.insert("key", field::Value::from(true))).is_ok();
    assert!(replaced, "existing field replacement failed");

    let table = (field::Value::table(table)).expect("valid replacement length");
    let table = match table.view() {
        view::Value::Table(table) => Some(table),
        _ => None,
    }
    .expect("expected replacement table");
    let entries = table.iter().collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    assert!(matches!(entries[0].1, view::Value::Bool(true)));
}
