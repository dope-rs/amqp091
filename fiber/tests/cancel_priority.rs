use std::convert::Infallible;
use std::io::Write as _;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::platform::wake;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once, wait};
pub mod common;

use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn orphan_preempts_owned() {
    let (seen, notify) = wake::Ends::blocking().expect("request observation").split();
    let (dropped, release) = wake::Ends::blocking().expect("drop barrier").split();
    let (retired, finish) = wake::Ends::blocking()
        .expect("retirement observation")
        .split();
    const OWNED: &str = "owned-cancel";
    const ORPHAN: &str = "orphan-cancel";

    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let owned_ok = consume_ok_arguments(OWNED).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &owned_ok).expect("AMQP frame fixture"))
                .expect("owned consume-ok");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            notify.notify().expect("orphan request");
            dropped
                .wait_timeout(Duration::from_secs(10))
                .expect("consumer drop");
            let orphan = consume_ok_arguments(ORPHAN).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &orphan).expect("AMQP frame fixture"))
                .expect("orphan consume-ok");

            check(&Method::read(stream).expect("AMQP method"), ORPHAN).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &orphan).expect("AMQP frame fixture"))
                .expect("orphan cancel-ok");
            check(&Method::read(stream).expect("AMQP method"), OWNED).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &owned_ok).expect("AMQP frame fixture"))
                .expect("owned cancel-ok");
            finish.notify().expect("consumers retired");
        },
        |broker| {
            let addr = broker.addr();

            let channel = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(2)
                .deliveries(1)
                .returns(1);
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(channel)
                .egress(config::Egress::new(2, 4096));
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(capacity);

            with_client(
                addr,
                config,
                |runtime| {
                    let conn = runtime.connection();
                    assert!(
                        matches!(runtime.block_on(conn.observe().ready()), Ok(Ok(_))),
                        "AMQP operation must succeed"
                    );
                    let channel = ((runtime.block_on(conn.channels().open(1)))
                        .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let args = Table::new();
                    let owned = ((runtime.block_on(channel.basic().consume(basic::Consume {
                        queue: "q",
                        consumer_tag: OWNED,
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: false,
                        arguments: &args,
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let mut orphan = Pinned::new(channel.basic().consume(basic::Consume {
                        queue: "q",
                        consumer_tag: ORPHAN,
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: false,
                        arguments: &args,
                    }));
                    assert!(
                        matches!(
                            runtime.block_on(poll_once(orphan.as_pin())),
                            Ok(std::task::Poll::Pending)
                        ),
                        "AMQP fiber must complete"
                    );
                    runtime
                        .block_on(wait(&seen))
                        .expect("fiber drive")
                        .expect("orphan request wait");
                    let (_, alloc) = Tracker::usage(|| {
                        drop(owned);
                        drop(orphan);
                    });
                    assert_eq!(alloc, (0, 0));
                    release.notify().expect("consumers dropped");
                    runtime
                        .block_on(wait(&retired))
                        .expect("fiber drive")
                        .expect("retirement wait");
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
    .expect("broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
