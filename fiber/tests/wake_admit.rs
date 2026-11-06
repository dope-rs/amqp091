use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope::core::driver::schedule;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
pub mod common;

use common::wake::Flow;
use dope::core::driver::schedule::ready::wake::{fanout, queue};
use dope_amqp091::failure::Error;
use dope_amqp091::protocol::method::{channel, queue as method_queue};
use dope_amqp091::protocol::wire::text::short;
use dope_amqp091::{adapter, failure};
use dope_fiber::task::batch::{Join, Pool};
use dope_fiber::task::local::Once;
use dope_test::harness::peer::TcpScript;

use crate::common::wake::{Fault, Open, Ready, TUNE, with_client};

#[test]
fn drains_disconnect() {
    const WIDTH: usize = schedule::LIMIT + 17;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(&mut *stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");
            Method::read(stream)
                .expect("second channel after every credit waiter registered")
                .check(2, 20, 10)
                .expect("channel.open route");
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                WIDTH,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let target = ((scope.block_on(Ready {
                        state: amqp.connection().observe(),
                        wait: fanout::Registration::new(),
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let lease = ((scope.block_on(Open {
                        adapter: amqp,
                        target,
                        lease: None,
                        call: None,
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");

                    let guard = ((scope.block_on(Once::new(move |local| {
                        amqp.methods().reserve(
                            local,
                            target,
                            lease,
                            method_queue::Purge {
                                queue: short::Name::EMPTY,
                            },
                        )
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let mut opening = scope
                        .block_on(Once::new(|mut access| {
                            let second = amqp
                                .channels()
                                .reserve(access.reborrow(), target, 2)?
                                .commit();
                            amqp.methods()
                                .reserve(access, target, second, channel::Open)
                        }))
                        .expect("fiber drive")
                        .expect("second channel reservation");
                    let mut head = Some(&mut opening);
                    // The last child opens channel 2; channel 1's guard remains unpolled.
                    let fibers = std::array::from_fn::<_, WIDTH, _>(|index| Flow {
                        adapter: amqp,
                        target: target.epoch(),
                        lease,
                        credit: queue::Registration::new(),
                        head: if index + 1 == WIDTH {
                            head.take()
                        } else {
                            None
                        },
                    });
                    let mut pool = ((scope.block_on(Pool::<WIDTH>::acquire()))
                        .expect("batch pool"))
                    .expect("batch pool");
                    let items = scope
                        .block_on(Join::from_array(&mut pool, fibers))
                        .expect("batch root")
                        .expect("batch root");
                    assert_eq!(items.len(), WIDTH);
                    let mut shared = None;
                    for item in items {
                        let (cause,) =
                            match item.expect_err("disconnect waiter lost its terminal cause") {
                                Fault::Runtime(failure::Runtime::Terminal(cause)) => Some((cause,)),
                                _ => None,
                            }
                            .expect("disconnect waiter lost its terminal cause");
                        assert_eq!(cause.generation(), 1);
                        std::assert_matches!(
                            cause.error(),
                            Error::Disconnected(failure::Reason::Remote)
                        );
                        match &shared {
                            Some(first) => assert!(cause.is_same(first)),
                            None => shared = Some(cause),
                        }
                    }
                    assert!(shared.is_some());
                    drop(guard);
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("broker completion");
        },
    )
    .expect("spawn disconnect broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
