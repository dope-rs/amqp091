use std::time;

use dope::core::driver::life::{creation::Attempt, recovery};
use dope::core::driver::route::KeyTag;
use dope::manifold::network::client::topology::plan;
use dope::manifold::network::client::topology::plan::Spec;
use dope::net::link::egress;
use dope::runtime::executor::{Executor, session};
use dope_amqp091::service::{Port, app::Plan};
use dope_amqp091::{config, service};

use crate::common::recover;
pub mod common;
use common::settings::{config, profile};

#[test]
fn binds_connector_policy() {
    let base = time::Duration::from_millis(1);
    let max = time::Duration::from_secs(1);
    let age = o3::base::time::Positive::from_secs::<60>();
    let config = (config()).expect("valid AMQP config");
    let store = service::Spec::<0>::try_new(config, 1, 4)
        .expect("valid AMQP storage plan")
        .with_age(age);
    let actual = (Plan::try_for(&store, base, max)).expect("valid AMQP connector plan");
    let expected = (plan::Plan::<KeyTag<0>, 1, plan::Standard, plan::External>::new(
        1,
        Spec::new(base, max)
            .expect("valid retry bounds")
            .with_ttl(age)
            .with_egress(egress::Config::shared(egress::Credit::new(
                egress::Entries::new(4),
                egress::Resident::new(16_384),
            ))),
    ))
    .expect("valid connector plan");

    assert!(actual.into_inner() == expected);
}

#[test]
fn preserves_retry_error() {
    let store = service::Spec::<0>::try_new(config().expect("valid AMQP config"), 1, 4)
        .expect("valid AMQP storage plan");
    let base = time::Duration::from_secs(2);
    let maximum = time::Duration::from_secs(1);
    assert!(matches!(
        Plan::try_for(&store, base, maximum),
        Err(service::app::Error::Backoff(
            dope::manifold::service::health::Error::Order { base: actual, maximum: limit }
        )) if actual == base && limit == maximum
    ));
}

#[test]
fn bounds_channel_stores() {
    let chan = config::Channel::new(1, 1, 1, 1, 1);
    let config = (config()).expect("valid AMQP config");
    let config = config
        .tune(config::Tune::default().channel_max(1))
        .capacity(
            config::Capacity::default()
                .channels(usize::MAX)
                .channel(chan)
                .body(config::Body::default().buffer(4096).fragments(4))
                .egress(config::Egress::new(4, 4096)),
        );
    let mut attempt = Attempt::new();
    let executor = Executor::new(&mut attempt, profile());
    assert!(executor.is_ok(), "executor creation");
    let executor = executor.ok().expect("executor creation");
    let plan = (service::Spec::<0>::try_new(config, 1, 4)).expect("valid AMQP storage plan");
    let mut reports = Vec::new();
    executor.run(|active| {
        let result = active.enter_with(
            |mut runtime| {
                Port::try_with(plan, &mut runtime, session::Scope::driver, |port, _| {
                    assert_eq!(
                        port.capacity().count().get(),
                        1,
                        "transport connection count"
                    );
                })
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "storage fixture performed I/O: {reports:?}"
        );
        assert!(
            matches!(result, Ok(Ok(()))),
            "AMQP storage allocation with bounded channels"
        );
    });
}
