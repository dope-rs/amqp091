use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_amqp091::message::property::Properties;
use dope_amqp091::{config, protocol};
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::app::plan;
use crate::common::amqp::wire::{Frame, Method};
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use dope_amqp091::failure;
use dope_test::harness::peer::TcpScript;

use crate::common::client::{channel, *};

#[test]
fn closes_pending_confirms() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"))
                .expect("channel.open-ok");
            confirm_select(stream, 1).expect("AMQP fixture");
            check_publish(stream, 1, "stale", b"stale").expect("AMQP fixture");
            check_publish(stream, 1, "parked", b"parked").expect("AMQP fixture");
            let mut nack = 2u64.to_be_bytes().to_vec();
            nack.push(2);
            stream
                .write_all(&Method::try_encode(1, 60, 120, &nack).expect("AMQP frame fixture"))
                .expect("parked NACK");
            Method::read(stream).expect("AMQP method").check(1, 50, 30).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(1, 50, 31, &0u32.to_be_bytes()).expect("AMQP frame fixture"))
                .expect("settlement barrier-ok");
            for key in ["waiting", "cancelled"] {
                check_publish(stream, 1, key, key.as_bytes()).expect("AMQP fixture");
            }
            Method::read(stream).expect("AMQP method").check(1, 20, 40).expect("AMQP method route");
            confirm_ack(stream, 1, 3).expect("late ACK fixture");
            for (class, method) in [(65535, 1), (20, 65535), (10, 30), (60, 120), (60, 50)] {
                stream
                    .write_all(&Method::try_encode(1, class, method, &[]).expect("AMQP frame fixture"))
                    .expect("discarded closing method");
            }
            for (kind, payload) in [(2, &[][..]), (3, &b"late"[..])] {
                stream
                    .write_all(&Frame::try_encode(kind, 1, payload).expect("AMQP frame fixture"))
                    .expect("discarded closing content");
            }
            stream
                .write_all(&Method::try_encode(1, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("channel.close-ok");

            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"))
                .expect("reused channel.open-ok");
            confirm_select(stream, 1).expect("AMQP fixture");
            check_publish(stream, 1, "current", b"current").expect("AMQP fixture");
            confirm_ack(stream, 1, 1).expect("AMQP fixture");
            Method::read(stream).expect("AMQP method").check(0, 10, 50).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection.close-ok");
        },
        |broker| {
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(config::Channel::default().confirms(4));
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("AMQP configuration").capacity(capacity),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel open");
                        let confirms = channel.confirm().select().await.expect("confirm.select");
                        let properties = Properties::default();
                        let mut stale = Pinned::new(confirms.publish(channel::request("", "stale", &properties, b"stale")));
                        assert!(poll_once(stale.as_pin()).await.is_pending());
                        let mut parked = Pinned::new(confirms.publish(channel::request("", "parked", &properties, b"parked")));
                        assert!(poll_once(parked.as_pin()).await.is_pending());
                        channel.queue().purge("barrier").await.expect("settlement barrier");
                        let mut waiting = Pinned::new(confirms.publish(channel::request("", "waiting", &properties, b"waiting")));
                        assert!(poll_once(waiting.as_pin()).await.is_pending());
                        let mut cancelled = Pinned::new(confirms.publish(channel::request("", "cancelled", &properties, b"cancelled")));
                        assert!(poll_once(cancelled.as_pin()).await.is_pending());
                        drop(cancelled);
                        channel.close(200, "done").await.expect("channel close");
                        assert!(matches!(waiting.await, Err(failure::Runtime::Local(failure::Error::Closed))));
                        assert_eq!(parked.await.expect("parked outcome"), protocol::Confirmation::Nack { requeue: true });

                        let channel = conn.channels().open(1).await.expect("reused channel slot");
                        let confirms = channel.confirm().select().await.expect("reused confirm.select");
                        let mut current = Pinned::new(confirms.publish(channel::request("", "current", &properties, b"current")));
                        assert!(poll_once(current.as_pin()).await.is_pending());
                        drop(stale);
                        assert_eq!(current.await.expect("current confirmation"), protocol::Confirmation::Ack);
                        conn.control().close(ready, 200, "done").await.expect("connection close");
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("channel-close confirm broker");
        },
    ).expect("spawn channel-close confirm broker");
}

#[test]
fn preserves_tag_sequence() {
    for capacity in [0, 1] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
                Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                    )
                    .expect("channel.open-ok");
                confirm_select(stream, 1).expect("AMQP fixture");

                if capacity != 0 {
                    check_publish(stream, 1, "first", b"one").expect("AMQP fixture");
                    confirm_ack(stream, 1, 1).expect("AMQP fixture");

                    let barrier = Method::read(stream).expect("AMQP method");
                    barrier.check(1, 50, 30).expect("AMQP method route");
                    assert_eq!(queue_name(&barrier).expect("AMQP fixture"), "barrier");
                    stream
                        .write_all(
                            &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                                .expect("AMQP frame fixture"),
                        )
                        .expect("barrier purge-ok");

                    check_publish(stream, 1, "second", b"two").expect("AMQP fixture");
                    confirm_ack(stream, 1, 2).expect("AMQP fixture");
                }
                Method::read(stream).expect("AMQP method").check(0, 10, 50).expect("AMQP method route");
                stream
                    .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                    .expect("connection.close-ok");
            },
            move |broker| {
                with(
                    broker.addr(),
                    plan::confirms(capacity).expect("AMQP configuration"),
                    |rt| {
                        let conn = rt.connection();
                        assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel open");
                            let confirms = channel.confirm().select().await.expect("confirm.select");
                            let properties = Properties::default();
                            let oversized_exchange = "x".repeat(256);

                            let (invalid, allocation) = Tracker::usage(|| {
                                confirms.publish(channel::request(
                                    &oversized_exchange,
                                    "never-sent",
                                    &properties,
                                    b"invalid",
                                ))
                            });
                            assert_eq!(allocation, (0, 0));
                            let invalid = invalid.await;
                            assert!(matches!(invalid, Err(ref error) if matches!(error.error(), failure::Error::Encoding(failure::wire::Encode::Length {
                                    kind: failure::wire::Encoding::Exchange,
                                    actual: 256,
                                    max: 127,
                                }))));

                            if capacity == 0 {
                                let rejected = confirms
                                    .publish(channel::request("", "never-sent", &properties, b"zero"))
                                    .await;
                                assert!(matches!(rejected, Err(ref error) if matches!(error.error(), failure::Error::Capacity(failure::wire::Capacity::PublishConfirm {
                                    used: 0,
                                    max,
                                }) if max.get() == 0)));
                            } else {
                                let (first, allocation) = Tracker::usage(|| {
                                    confirms.publish(channel::request("", "first", &properties, b"one"))
                                });
                                assert_eq!(allocation, (0, 0));
                                let mut first = Pinned::new(first);
                                assert!(poll_once(first.as_pin()).await.is_pending());
                                let rejected = confirms
                                    .publish(channel::request("", "never-sent", &properties, b"full"))
                                    .await;
                                assert!(matches!(rejected, Err(ref error) if matches!(error.error(), failure::Error::Capacity(failure::wire::Capacity::PublishConfirm {
                                    used: 1,
                                    max,
                                }) if max.get() == 1)));
                                let (_, allocation) = Tracker::usage(|| drop(first));
                                assert_eq!(allocation, (0, 0));

                                channel
                                    .queue()
                                    .purge("barrier")
                                    .await
                                    .expect("ACK dispatch barrier");
                                let outcome = confirms
                                    .publish(channel::request("", "second", &properties, b"two"))
                                    .await
                                    .expect("second confirm");
                                assert_eq!(outcome, protocol::Confirmation::Ack);
                            }
                            conn
                                .control()
                                .close(ready, 200, "done")
                                .await
                                .expect("connection close");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                    }, recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                broker.finish().expect("confirm sequence broker");
            },
        )
        .expect("spawn confirm-sequence broker");
    }
}

#[test]
fn reclaims_bulk_capacity() {
    const CONFIRM_CAPACITY: usize = 3;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");
            confirm_select(stream, 1).expect("AMQP fixture");

            check_publish(stream, 1, "ack-dropped", b"dropped").expect("AMQP fixture");
            check_publish(stream, 1, "ack-first", b"first").expect("AMQP fixture");
            check_publish(stream, 1, "ack-second", b"second").expect("AMQP fixture");
            confirm_ack_all(stream, 1).expect("AMQP fixture");

            check_publish(stream, 1, "nack-first", b"first").expect("AMQP fixture");
            check_publish(stream, 1, "nack-second", b"second").expect("AMQP fixture");
            check_publish(stream, 1, "nack-third", b"third").expect("AMQP fixture");
            confirm_nack_all(stream, 1, true).expect("AMQP fixture");

            check_publish(stream, 1, "reused-first", b"first").expect("AMQP fixture");
            check_publish(stream, 1, "reused-second", b"second").expect("AMQP fixture");
            check_publish(stream, 1, "reused-third", b"third").expect("AMQP fixture");
            confirm_ack_all(stream, 1).expect("AMQP fixture");

            Method::read(stream).expect("AMQP method").check(0, 10, 50).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection.close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                plan::confirms(CONFIRM_CAPACITY).expect("AMQP configuration"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel open");
                            let confirms = channel.confirm().select().await.expect("confirm.select");
                            let properties = Properties::default();

                            let mut dropped = Pinned::new(confirms.publish(channel::request(
                                "",
                                "ack-dropped",
                                &properties,
                                b"dropped",
                            )));
                            assert!(poll_once(dropped.as_pin()).await.is_pending());
                            drop(dropped);
                            let mut ack_first = Pinned::new(confirms.publish(channel::request(
                                "",
                                "ack-first",
                                &properties,
                                b"first",
                            )));
                            assert!(poll_once(ack_first.as_pin()).await.is_pending());
                            let mut ack_second = Pinned::new(confirms.publish(channel::request(
                                "",
                                "ack-second",
                                &properties,
                                b"second",
                            )));
                            assert!(poll_once(ack_second.as_pin()).await.is_pending());
                            let first_outcome = ack_first.await.expect("first bulk ACK");
                            assert_eq!(first_outcome, protocol::Confirmation::Ack);
                            let second_outcome = ack_second.await.expect("second bulk ACK");
                            assert_eq!(second_outcome, protocol::Confirmation::Ack);

                            let mut nack_first = Pinned::new(confirms.publish(channel::request(
                                "",
                                "nack-first",
                                &properties,
                                b"first",
                            )));
                            assert!(poll_once(nack_first.as_pin()).await.is_pending());
                            let mut nack_second = Pinned::new(confirms.publish(channel::request(
                                "",
                                "nack-second",
                                &properties,
                                b"second",
                            )));
                            assert!(poll_once(nack_second.as_pin()).await.is_pending());
                            let mut nack_third = Pinned::new(confirms.publish(channel::request(
                                "",
                                "nack-third",
                                &properties,
                                b"third",
                            )));
                            assert!(poll_once(nack_third.as_pin()).await.is_pending());
                            let expected_nack = protocol::Confirmation::Nack { requeue: true };
                            let first_outcome = nack_first.await.expect("first bulk NACK");
                            assert_eq!(first_outcome, expected_nack);
                            let second_outcome = nack_second.await.expect("second bulk NACK");
                            assert_eq!(second_outcome, expected_nack);
                            let third_outcome = nack_third.await.expect("third bulk NACK");
                            assert_eq!(third_outcome, expected_nack);

                            let mut reused_first = Pinned::new(confirms.publish(channel::request(
                                "",
                                "reused-first",
                                &properties,
                                b"first",
                            )));
                            assert!(poll_once(reused_first.as_pin()).await.is_pending());
                            let mut reused_second = Pinned::new(confirms.publish(channel::request(
                                "",
                                "reused-second",
                                &properties,
                                b"second",
                            )));
                            assert!(poll_once(reused_second.as_pin()).await.is_pending());
                            let mut reused_third = Pinned::new(confirms.publish(channel::request(
                                "",
                                "reused-third",
                                &properties,
                                b"third",
                            )));
                            assert!(poll_once(reused_third.as_pin()).await.is_pending());
                            let first_outcome = reused_first.await.expect("first reused ACK");
                            assert_eq!(first_outcome, protocol::Confirmation::Ack);
                            let second_outcome = reused_second.await.expect("second reused ACK");
                            assert_eq!(second_outcome, protocol::Confirmation::Ack);
                            let third_outcome = reused_third.await.expect("third reused ACK");
                            assert_eq!(third_outcome, protocol::Confirmation::Ack);

                            conn
                                .control()
                                .close(ready, 200, "done")
                                .await
                                .expect("connection close");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("zero-tag bulk confirm broker");
        },
    )
    .expect("spawn bulk-confirm broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
