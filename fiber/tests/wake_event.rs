use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope::core::driver::schedule;
use dope_test::harness::support::gates::Barrier;

use crate::common::amqp::Peer;
pub mod common;

use std::num::NonZero;
use std::time::Duration;

use dope::core::driver::schedule::ready::wake::fanout;
use dope::core::platform::wake;
use dope_amqp091::adapter;
use dope_fiber::task::batch::{Join, Pool};
use dope_test::harness::peer::TcpScript;

use crate::common::fiber::{Pinned, poll_once};
use crate::common::wake::{Ready, TUNE, with_client};

#[test]
fn drains_budget() {
    let (end, done) = wake::Ends::blocking().expect("completion pipe").split();
    const WIDTH: usize = schedule::LIMIT + 17;

    let (open, notify) = wake::Ends::blocking().expect("arrival pipe").split();
    let gate = Barrier::new(
        NonZero::<usize>::new(WIDTH).expect("waiter width is positive"),
        Some(notify),
    );
    TcpScript::with(
        move |stream| {
            open.wait_timeout(Duration::from_secs(10))
                .expect("test gate");
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            end.wait_timeout(Duration::from_secs(10))
                .expect("test gate");
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                WIDTH,
                |scope| {
                    let state = adapter::Adapter::new(scope.port()).connection().observe();
                    let mut fibers = [(); WIDTH].map(|()| {
                        Pinned::new(Ready {
                            state,
                            wait: fanout::Registration::new(),
                            gate: Some(&gate),
                        })
                    });
                    assert!(
                        matches!(
                            scope.block_on(poll_once(fibers[0].as_pin())),
                            Ok(std::task::Poll::Pending)
                        ),
                        "AMQP fiber must complete"
                    );
                    let items = ((scope.block_on(dope_fiber::fiber!('_ => async move {
                        let mut pool = Pool::<WIDTH>::acquire().await?;
                        Join::from_array(&mut pool, fibers).await
                    })))
                    .expect("ready waiter batch"))
                    .expect("ready waiter batch");
                    assert_eq!(items.len(), WIDTH);
                    assert!(items.into_iter().all(|item| item.is_ok()));
                    done.notify().expect("release broker");
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
    .expect("spawn handshake-event broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
