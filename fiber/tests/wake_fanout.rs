use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_test::alloc;
use dope_test::harness::scenario::process::Deadline;
use dope_test::harness::support::gates::Barrier;

use crate::common::amqp::Peer;
pub mod common;
use std::mem;
use std::num::NonZero;

use common::wake::progress::{Phase, Progress};
use dope::core::driver::schedule::ready::wake::fanout;
use dope::core::platform::wake;
use dope_amqp091::adapter;
use dope_fiber::task::batch::{Join, Pool};
use dope_test::alloc::Tracker;
use dope_test::harness::peer::TcpScript;

use crate::common::wake::{TUNE, with_client};

#[test]
fn stays_allocation_free() {
    const WIDTH: usize = 8;

    let gate = Barrier::new(
        NonZero::<usize>::new(WIDTH).expect("waiter width is positive"),
        None,
    );
    let (peer, end) = wake::Ends::blocking().expect("completion pipe").split();
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            peer.wait_timeout(Duration::from_secs(10))
                .expect("test gate");
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                WIDTH,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let observe = scope.connection().observe();
                    let ready = observe.ready();
                    assert_eq!(mem::size_of_val(&ready), 3 * mem::size_of::<usize>());
                    let target = ((scope.block_on(ready))
                        .expect("AMQP connection must become ready"))
                    .expect("AMQP connection must become ready");
                    let epoch = observe.epoch();
                    assert_eq!(mem::size_of_val(&epoch), 3 * mem::size_of::<usize>());
                    let epoch = ((scope.block_on(epoch))
                        .expect("ready connection must retain its epoch"))
                    .expect("ready connection must retain its epoch");
                    assert!(
                        epoch == target.epoch(),
                        "ready connection must retain the same epoch"
                    );
                    let group = amqp.channels();
                    let fibers = [(); WIDTH].map(|()| Progress {
                        channels: group,
                        connection: amqp.connection(),
                        target: target.epoch(),
                        wait: fanout::Registration::new(),
                        phase: Phase::Wait,
                        gate: &gate,
                    });
                    let mut pool = ((scope.block_on(Pool::<WIDTH>::acquire()))
                        .expect("batch pool"))
                    .expect("batch pool");
                    let batch = Join::from_array(&mut pool, fibers);
                    let (items, alloc) = Deadline::with(Duration::from_secs(10), |_| {
                        let (items, alloc) = Tracker::usage(|| scope.drive(batch));
                        let items = ((items).expect("batch root")).expect("batch root");
                        (items, alloc)
                    })
                    .expect("completion deadline");
                    assert_eq!(alloc, (0, 0));
                    assert!(items.into_iter().all(|item| item.is_ok()));
                    end.notify().expect("release broker");
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
    .expect("spawn recovery fanout broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
