use std::convert::Infallible;
use std::io::Write as _;

use dope::core::driver::life::recovery;
use dope::core::driver::schedule;
use dope_test::harness::support::gates::Barrier;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
pub mod common;

use std::num::NonZero;
use std::time::Duration;

use common::wake::Flow;
use dope::core::driver::schedule::ready::wake::{fanout, queue};
use dope::core::platform::wake;
use dope_amqp091::failure::Error;
use dope_amqp091::protocol::method::channel;
use dope_amqp091::{adapter, failure};
use dope_fiber::task::batch::{Join, Pool};
use dope_fiber::task::local::Once;
use dope_test::harness::peer::TcpScript;

use crate::common::wake::{Fault, Open, Ready, TUNE, with_client};

#[test]
fn drains_disconnect() {
    const WIDTH: usize = schedule::LIMIT + 17;

    let (open, notify) = wake::Ends::blocking().expect("arrival pipe").split();
    let gate = Barrier::new(
        NonZero::<usize>::new(WIDTH).expect("waiter width is positive"),
        Some(notify),
    );
    TcpScript::with(
        move |stream| {
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
            open.wait_timeout(Duration::from_secs(10))
                .expect("test gate");
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
                        gate: None,
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
                        amqp.methods().reserve(local, target, lease, channel::Open)
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let fibers = [(); WIDTH].map(|()| Flow {
                        adapter: amqp,
                        target: target.epoch(),
                        lease,
                        credit: queue::Registration::new(),
                        gate: Some(&gate),
                    });
                    let mut pool = ((scope.block_on(Pool::<WIDTH>::acquire()))
                        .expect("batch pool"))
                    .expect("batch pool");
                    let items = ((scope.block_on(Join::from_array(&mut pool, fibers)))
                        .expect("batch root"))
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
