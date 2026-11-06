use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::adapter::Poll as _;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091::protocol::method::{channel as method_channel, queue};
use dope_amqp091::protocol::wire::text::short;
use dope_amqp091::{adapter, config, failure};
use dope_amqp091_fiber::channel::basic;
use dope_fiber::abi::poll_fn;
use dope_fiber::task::local::Once;
use dope_test::alloc;
use dope_test::alloc::Tracker;
use o3::base::cell::local;

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};
use crate::common::fiber::{Pinned, poll_once};
pub mod common;
use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn compensates_dropped_ops() {
    const BEFORE: &str = "drop-before-ok";
    const AFTER: &str = "drop-after-ok";

    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(Tune::new(2, 4096, 0))
                .expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");

            check_client_consume(&Method::read(stream).expect("before consume"), BEFORE)
                .expect("before consume route and tag");
            Method::read(stream)
                .expect("observation channel open")
                .check(2, 20, 10)
                .expect("observation channel route");
            stream
                .write_all(
                    &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("consume request observed");
            Method::read(stream)
                .expect("post-drop purge")
                .check(2, 50, 30)
                .expect("post-drop barrier route");
            let before = consume_ok_arguments(BEFORE).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &before).expect("AMQP frame fixture"))
                .expect("consume-ok after future drop");
            check(&Method::read(stream).expect("AMQP method"), BEFORE).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &before).expect("AMQP frame fixture"))
                .expect("before cancel-ok");
            stream
                .write_all(
                    &Method::try_encode(2, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("before cancellation processed");

            check_client_consume(&Method::read(stream).expect("after consume"), AFTER)
                .expect("after consume route and tag");
            Method::read(stream)
                .expect("response observation purge")
                .check(2, 50, 30)
                .expect("response observation route");
            let after = consume_ok_arguments(AFTER).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &after).expect("AMQP frame fixture"))
                .expect("consume-ok before future drop");
            stream
                .write_all(
                    &Method::try_encode(2, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("consume-ok response barrier");
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
                .channels(2)
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
                    // The peer opens this channel only after reading Consume on channel 1.
                    let barrier = rt
                        .block_on(conn.channels().open(2))
                        .expect("fiber drive")
                        .expect("consume request observed");
                    let mut closing = Pinned::new(channel.close(200, "done"));
                    assert!(
                        rt.block_on(poll_once(closing.as_pin()))
                            .expect("closing reservation")
                            .is_pending()
                    );
                    let (_, alloc) = Tracker::usage(|| drop(before));
                    assert_eq!(alloc, (0, 0));
                    // The peer replies after ConsumeOk, exact Cancel, and CancelOk on channel 1.
                    rt.block_on(barrier.queue().purge("dropped"))
                        .expect("fiber drive")
                        .expect("before cancellation processed");
                    drop(closing);

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
                    rt.block_on(barrier.queue().purge("replied"))
                        .expect("fiber drive")
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

#[test]
fn consumer_drop_precedes_unsent_close() {
    const TAG: &str = "drop-during-close";

    for (held_purge, cancel_close) in [(true, false), (true, true), (false, false)] {
        TcpScript::with(
            move |stream| {
                stream.read_wait(Some(Duration::from_secs(10))).expect("read deadline");
                Peer::new(stream).open(Tune::new(2, 4096, 0)).expect("handshake");
                Method::read(stream).expect("channel open").check(1, 20, 10)
                    .expect("channel.open");
                Method::write(stream, 1, 20, 11, &[0; 4]).expect("channel.open-ok");
                check_client_consume(&Method::read(stream).expect("consume"), TAG)
                    .expect("consumer tag");
                let tag = consume_ok_arguments(TAG).expect("consumer tag");
                Method::write(stream, 1, 60, 21, &tag).expect("consume-ok");
                Method::read(stream).expect("barrier open").check(2, 20, 10)
                    .expect("barrier channel.open");
                Method::write(stream, 2, 20, 11, &[0; 4]).expect("barrier channel.open-ok");
                let cancel = if held_purge {
                    Method::read(stream).expect("pending purge").check(1, 50, 30)
                        .expect("head queue.purge");
                    Method::read(stream).expect("consumer drop barrier").check(2, 50, 30)
                        .expect("drop barrier queue.purge");
                    Method::write(stream, 2, 50, 31, &[0; 4]).expect("drop barrier purge-ok");
                    Method::read(stream).expect("close decision barrier").check(2, 50, 30)
                        .expect("decision barrier queue.purge");
                    Method::write(stream, 1, 50, 31, &[0; 4]).expect("head purge-ok");
                    Method::read(stream).expect("consumer cancel before close")
                } else {
                    let first = Method::read(stream).expect("consumer cancel or barrier");
                    let second = Method::read(stream).expect("remaining cancel or barrier");
                    let (cancel, barrier) = if first.0 == 1 {
                        (first, second)
                    } else {
                        (second, first)
                    };
                    barrier.check(2, 50, 30).expect("drop barrier queue.purge");
                    cancel
                };
                check(&cancel, TAG).expect("consumer cancel");
                Method::write(stream, 1, 60, 31, &tag).expect("cancel-ok");
                Method::write(stream, 2, 50, 31, &[0; 4]).expect("decision barrier purge-ok");
                let next = Method::read(stream).expect("operation after consumer cancel");
                if cancel_close {
                    next.check(1, 50, 30).expect("cancelled close emitted a frame");
                    Method::write(stream, 1, 50, 31, &[0; 4]).expect("next purge-ok");
                } else {
                    next.check(1, 20, 40).expect("channel.close");
                    Method::write(stream, 1, 20, 41, &[]).expect("channel.close-ok");
                }
                Method::read(stream).expect("connection close").check(0, 10, 50)
                    .expect("stray channel operation");
                Method::write(stream, 0, 10, 51, &[]).expect("connection.close-ok");
            },
            |broker| {
                let channel = config::Channel::default().methods(2).confirms(1)
                    .consumers(1).deliveries(1).returns(1);
                let capacity = config::Capacity::default().channels(2).channel(channel)
                    .egress(config::Egress::new(2, 4096));
                with_client(
                    broker.addr(),
                    config::Config::new("guest", "guest").expect("config").capacity(capacity),
                    |rt| {
                        let conn = rt.connection();
                        let amqp = adapter::Adapter::new(rt.port());
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            let args = Table::new();
                            let consumer = channel.basic().consume(basic::Consume {
                                queue: "q",
                                consumer_tag: TAG,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            }).await.expect("consumer");
                            let barrier = conn.channels().open(2).await.expect("barrier channel");
                            let mut first = held_purge.then(|| Pinned::new(channel.queue().purge("head")));
                            if let Some(first) = &mut first {
                                assert!(poll_once(first.as_pin()).await.is_pending());
                            }
                            let close = Once::new(|mut access| {
                                let lease = amqp.channels().current(access.reborrow(), 1)
                                    .expect("open channel");
                                amqp.methods().reserve(access, ready, lease, method_channel::CloseRef {
                                    reply_code: 200,
                                    reply_text: short::Short::EMPTY,
                                    class_id: 0,
                                    method_id: 0,
                                })
                            }).await.expect("unpolled close reservation");
                            let mut close = Some(close);
                            drop(consumer);
                            barrier.queue().purge("dropped").await.expect("consumer drop barrier");
                            if cancel_close {
                                drop(close.take());
                            }
                            if held_purge {
                                barrier.queue().purge("continue").await.expect("close decision barrier");
                            }
                            if let Some(first) = first {
                                first.await.expect("head purge");
                            }
                            if cancel_close {
                                channel.queue().purge("next").await.expect("purge after cancelled close");
                            } else {
                                poll_fn(|mut cx| {
                                    let wake = cx.as_ref().completion();
                                    let access = local::Access::new(cx.as_mut().region_token());
                                    close.as_mut().expect("retained close").poll(access, wake)
                                }).await.expect("close after consumer cancel");
                            }
                            conn.control().close(ready, 200, "done").await.expect("connection close");
                        })).expect("fiber completion");
                    },
                    recover,
                    |result| {
                        result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                    },
                );
                broker.finish().expect("broker");
            },
        ).expect("consumer close ordering broker");
    }
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
