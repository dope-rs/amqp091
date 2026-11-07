use std::task::Poll;

use dope::core::driver::schedule;

use crate::common::amqp::Peer;
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::{adapter, failure};
use dope_fiber::abi::poll_fn;
use dope_fiber::task::batch::{Join, Pool};
use dope_test::harness::peer::TcpScript;
use o3::base::cell::local;

use crate::common::wake::{Ready, TUNE, with_client};

#[test]
fn drains_budget() {
    const WIDTH: usize = schedule::LIMIT + 17;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            common::basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                WIDTH,
                |scope| {
                    let connection = adapter::Adapter::new(scope.port()).connection();
                    let state = connection.observe();
                    let target = scope
                        .block_on(Ready {
                            state,
                            wait: fanout::Registration::new(),
                        })
                        .expect("fiber drive")
                        .expect("connection ready");
                    let fibers = std::array::from_fn::<_, WIDTH, _>(|index| {
                        let mut wait = fanout::Registration::new();
                        let mut retire = index + 1 == WIDTH;
                        poll_fn(move |mut cx| {
                            let wake = cx.as_ref().completion();
                            let access = local::Access::new(cx.as_mut().region_token());
                            let result =
                                state.poll_retired(access, target.epoch(), &mut wait, wake);
                            // Join's ordered first polls register every waiter before this event.
                            if std::mem::take(&mut retire) {
                                assert!(result.is_pending(), "last waiter must be registered");
                                let mut start = connection.control().start(target, 200, "bad\0");
                                let mut close = connection.control().close(target, 200, "bad\0");
                                for first in [true, false] {
                                    let start_wake = cx.as_ref().completion();
                                    let close_wake = cx.as_ref().completion();
                                    let mut access = local::Access::new(cx.as_mut().region_token());
                                    for result in [
                                        start.poll(access.reborrow(), start_wake),
                                        close.poll(access, close_wake),
                                    ] {
                                        let Poll::Ready(Err(error)) = result else {
                                            panic!("invalid close did not fail");
                                        };
                                        assert!(matches!(
                                            (first, error.error()),
                                            (
                                                true,
                                                failure::Error::Protocol(
                                                    failure::wire::Fault::TextNul {
                                                        kind: failure::wire::Encoding::Short,
                                                        offset: 3,
                                                    }
                                                )
                                            ) | (false, failure::Error::Polled)
                                        ));
                                    }
                                }
                                let wake = cx.as_ref().completion();
                                let access = local::Access::new(cx.as_mut().region_token());
                                assert!(
                                    matches!(
                                        connection
                                            .control()
                                            .start(target, 200, "done")
                                            .poll(access, wake),
                                        Poll::Ready(Ok(()))
                                    ),
                                    "start connection close"
                                );
                            }
                            result
                        })
                    });
                    let mut pool = scope
                        .block_on(Pool::<WIDTH>::acquire())
                        .expect("fiber drive")
                        .expect("batch pool");
                    let items = scope
                        .block_on(Join::from_array(&mut pool, fibers))
                        .expect("batch root")
                        .expect("retirement waiter batch");
                    assert_eq!(items.len(), WIDTH);
                    assert!(items.into_iter().all(|item| item.is_ok()));
                },
                recover,
                complete,
            );
            broker.finish().expect("broker completion");
        },
    )
    .expect("spawn connection-event broker");
}
