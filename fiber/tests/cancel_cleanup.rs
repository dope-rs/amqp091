use std::convert::Infallible;
use std::io::Write as _;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::platform::wake;
use dope_amqp091::adapter::Poll as _;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091::protocol::method::{channel as method_channel, queue};
use dope_amqp091::protocol::wire::text::short;
use dope_amqp091::{adapter, config, failure};
use dope_amqp091_fiber::channel::basic;
use dope_fiber::abi::poll_fn;
use dope_fiber::task::local::Once;
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::alloc;
use dope_test::alloc::Tracker;
use o3::base::cell::local;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once, wait};
pub mod common;
use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn compensates_dropped_ops() {
    let (observed, notify) = wake::Ends::blocking().expect("broker observations").split();
    let (dropped, release) = wake::Ends::blocking().expect("drop barrier").split();
    const BEFORE: &str = "drop-before-ok";
    const AFTER: &str = "drop-after-ok";

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
            notify.notify().expect("before request");
            dropped
                .wait_timeout(Duration::from_secs(10))
                .expect("future drop");
            let before = consume_ok_arguments(BEFORE).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &before).expect("AMQP frame fixture"))
                .expect("consume-ok after future drop");
            check(&Method::read(stream).expect("AMQP method"), BEFORE).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &before).expect("AMQP frame fixture"))
                .expect("before cancel-ok");
            notify.notify().expect("before cancel-ok written");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let after = consume_ok_arguments(AFTER).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &after).expect("AMQP frame fixture"))
                .expect("consume-ok before future drop");
            stream
                .write_all(
                    &Method::try_encode(0, 10, 60, b"\x05ready").expect("AMQP frame fixture"),
                )
                .expect("connection.blocked barrier");
            check(&Method::read(stream).expect("AMQP method"), AFTER).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &after).expect("AMQP frame fixture"))
                .expect("after cancel-ok");
            Method::read(stream)
                .expect("channel.close after cancel")
                .check(1, 20, 40)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(1, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("channel.close-ok");
        },
        move |broker| {
            let chan = config::Channel::default()
                .methods(2)
                .confirms(1)
                .consumers(1)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    let target = rt
                        .block_on(conn.observe().ready())
                        .expect("fiber drive")
                        .expect("connection ready");
                    let amqp = adapter::Adapter::new(rt.port());
                    let channel = rt
                        .block_on(conn.channels().open(1))
                        .expect("fiber drive")
                        .expect("AMQP operation must succeed");
                    let args = Table::new();
                    let timer = rt.timer();

                    let mut before = Pinned::new(channel.basic().consume(basic::Consume {
                        queue: "q",
                        consumer_tag: BEFORE,
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: false,
                        arguments: &args,
                    }));
                    assert!(
                        matches!(
                            rt.block_on(poll_once(before.as_pin())),
                            Ok(std::task::Poll::Pending)
                        ),
                        "AMQP fiber must complete"
                    );
                    rt.block_on(wait(&observed))
                        .expect("fiber drive")
                        .expect("before request wait");
                    let (_, alloc) = Tracker::usage(|| drop(before));
                    assert_eq!(alloc, (0, 0));
                    release.notify().expect("future dropped");
                    rt.block_on(wait(&observed))
                        .expect("fiber drive")
                        .expect("before cancel-ok written");

                    let mut after = Pinned::new(channel.basic().consume(basic::Consume {
                        queue: "q",
                        consumer_tag: AFTER,
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: false,
                        arguments: &args,
                    }));
                    assert!(
                        matches!(
                            rt.block_on(poll_once(after.as_pin())),
                            Ok(std::task::Poll::Pending)
                        ),
                        "AMQP fiber must complete"
                    );
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        while !conn.observe().read(|view| view.is_blocked()).await {
                            timer
                                .sleep(Duration::from_millis(1))
                                .expect("response barrier wait")
                                .await;
                        }
                    }))
                    .expect("consume-ok response barrier");
                    let mut close = rt
                        .block_on(Once::new(move |mut access| {
                            let lease = amqp
                                .channels()
                                .current(access.reborrow(), 1)
                                .expect("open channel");
                            let (_, alloc) = Tracker::usage(|| drop(after));
                            assert_eq!(alloc, (0, 0));
                            let pending = amqp
                                .methods()
                                .reserve(
                                    access.reborrow(),
                                    target,
                                    lease,
                                    queue::Purge {
                                        queue: short::Name::EMPTY,
                                    },
                                )
                                .expect("reservation queues consumer retirement");
                            let close = amqp
                                .methods()
                                .reserve(
                                    access.reborrow(),
                                    target,
                                    lease,
                                    method_channel::CloseRef {
                                        reply_code: 200,
                                        reply_text: short::Short::EMPTY,
                                        class_id: 0,
                                        method_id: 0,
                                    },
                                )
                                .expect("close reservation");
                            drop(pending);
                            assert!(matches!(
                                amqp.methods().reserve(
                                    access,
                                    target,
                                    lease,
                                    queue::Purge {
                                        queue: short::Name::EMPTY,
                                    },
                                ),
                                Err(failure::Runtime::Local(failure::Error::Closed))
                            ));
                            close
                        }))
                        .expect("closing reservation turn");
                    rt.block_on(poll_fn(|mut cx| {
                        let wake = cx.as_ref().completion();
                        let access = local::Access::new(cx.as_mut().region_token());
                        close.poll(access, wake)
                    }))
                    .expect("fiber drive")
                    .expect("close after consumer cancellation");
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
