use dope_test::alloc;

use crate::common::amqp::Peer;
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use common::wake::progress::{Phase, Progress};
use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter;
use dope_fiber::task::batch::{Join, Pool};
use dope_test::alloc::Tracker;
use dope_test::harness::peer::TcpScript;

use crate::common::wake::{TUNE, with_client};

#[test]
fn stays_allocation_free() {
    const WIDTH: usize = 8;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            assert_eq!(stream.read(&mut [0]).expect("connection retirement"), 0);
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                WIDTH,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let observe = scope.connection().observe();
                    let target = ((scope.block_on(observe.ready()))
                        .expect("AMQP connection must become ready"))
                    .expect("AMQP connection must become ready");
                    let epoch = ((scope.block_on(observe.epoch()))
                        .expect("ready connection must retain its epoch"))
                    .expect("ready connection must retain its epoch");
                    assert!(
                        epoch == target.epoch(),
                        "ready connection must retain the same epoch"
                    );
                    let group = amqp.channels();
                    // Join polls fresh children in order, before revisiting any waiter.
                    let fibers = std::array::from_fn::<_, WIDTH, _>(|index| Progress {
                        channels: group,
                        connection: amqp.connection(),
                        target: target.epoch(),
                        wait: fanout::Registration::new(),
                        phase: if index + 1 == WIDTH {
                            Phase::Retire
                        } else {
                            Phase::Wait
                        },
                    });
                    let mut pool = ((scope.block_on(Pool::<WIDTH>::acquire()))
                        .expect("batch pool"))
                    .expect("batch pool");
                    let batch = Join::from_array(&mut pool, fibers);
                    let (items, alloc) = Tracker::usage(|| scope.block_on(batch));
                    let items = items.expect("batch root").expect("batch root");
                    assert_eq!(alloc, (0, 0));
                    assert!(items.into_iter().all(|item| item.is_ok()));
                },
                recover,
                complete,
            );
            broker.finish().expect("broker completion");
        },
    )
    .expect("spawn recovery fanout broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();
