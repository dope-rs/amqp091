use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_test::alloc;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::client::{check_publish, confirm_ack, confirm_select};
pub mod common;

use std::time::Duration;

use dope_amqp091::adapter::publish;
use dope_amqp091::message::property::Properties;
use dope_amqp091::{config, failure, protocol};
use dope_test::alloc::Tracker;
use dope_test::fibers::Yield;
use dope_test::harness::peer::{AcceptScript, TcpScript};

use crate::common::fiber::{Pinned, poll_once};
use crate::common::publish::{TUNE, with_client, with_config};

#[test]
fn rejects_plain_after_select() {
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            let channel = common::basic::returned::open_channel(stream).expect("channel");
            Method::read(stream)
                .expect("confirm.select")
                .check(channel, 85, 10)
                .expect("confirm method");
            Method::write(stream, channel, 85, 11, &[]).expect("confirm.select-ok");
            check_publish(stream, channel, "confirmed", b"b").expect("only confirmed publish");
            confirm_ack(stream, channel, 1).expect("first confirmation");
            common::basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            with_client(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let props = Properties::default();
                        let mut plain = Pinned::new(channel.basic().publish(publish::Request {
                            exchange: "",
                            routing_key: "plain",
                            mandatory: false,
                            properties: &props,
                            body: b"a",
                        }));
                        let mut select = Pinned::new(channel.confirm().select());
                        // Both polls complete locally before this outer fiber yields.
                        assert!(poll_once(select.as_pin()).await.is_pending());
                        assert!(poll_once(plain.as_pin()).await.is_pending());
                        let confirms = select.await.expect("confirm.select");
                        let error = plain.await.expect_err("plain publish after confirm selection");
                        assert!(matches!(error, failure::Runtime::Local(failure::Error::Protocol(
                            failure::wire::Fault::ConfirmPublishRequired
                        ))));
                        let outcome = confirms.publish(publish::Request {
                            exchange: "",
                            routing_key: "confirmed",
                            mandatory: false,
                            properties: &props,
                            body: b"b",
                        }).await.expect("confirmed publish");
                        assert_eq!(outcome, protocol::Confirmation::Ack);
                        conn.control().close(ready, 200, "done").await.expect("close");
                    }))
                    .expect("fiber");
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
fn keeps_partial_ack_before_later_publish() {
    use dope_amqp091::message::{ack, field::Table};
    use dope_amqp091_fiber::channel::basic;

    for short_prefix in [true, false] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("handshake");
                let channel = common::basic::returned::open_channel(stream).expect("channel");
                common::cancel::check_client_consume(
                    &Method::read(stream).expect("receipt consumer"), "receipt",
                ).expect("consumer tag");
                let tag = common::cancel::consume_ok_arguments("receipt").expect("consumer tag");
                Method::write(stream, channel, 60, 21, &tag).expect("consume-ok");
                stream.write_all(
                    &common::cancel::delivery_frames_with_tag(channel, "receipt", 1)
                        .expect("delivery fixture"),
                ).expect("receipt delivery");
                confirm_select(stream, channel).expect("confirm.select");
                check_publish(stream, channel, "first", b"first").expect("first publish");
                Method::read(stream)
                    .expect("barrier")
                    .check(channel, 50, 30)
                    .expect("purge route");
                let mut args = [0; 9];
                args[8] = 1;
                let ack = Method::try_encode(channel, 60, 80, &args).expect("bulk ACK");
                let split = if short_prefix { 2 } else { ack.len() - 1 };
                let mut prefix = Method::try_encode(channel, 50, 31, &0u32.to_be_bytes())
                    .expect("barrier reply");
                prefix.extend_from_slice(&ack[..split]);
                stream.write_all(&prefix).expect("barrier and partial ACK");
                // A bound prefix permits later publication under its retained watermark.
                if !short_prefix {
                    check_publish(stream, channel, "second", b"second").expect("second publish");
                }
                let receipt = Method::read(stream).expect("settlement after later publish poll");
                receipt.check(channel, 60, 80).expect("basic.ack");
                assert_eq!(receipt.arguments(), &[0, 0, 0, 0, 0, 0, 0, 1, 0]);
                stream.write_all(&ack[split..]).expect("ACK suffix");
                if short_prefix {
                    check_publish(stream, channel, "second", b"second").expect("second publish");
                }
                Method::write_parts(stream, channel, 60, 120, &[&2u64.to_be_bytes(), &[0]])
                    .expect("second publish NACK");
                common::basic::returned::close_connection(stream).expect("connection close");
            },
            |broker| {
                with_config(
                    broker.addr(),
                    common::amqp::app::plan::confirms(2).expect("config"),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            let arguments = Table::new();
                            let mut consumer = channel.basic().consume(basic::Consume {
                                queue: "q",
                                consumer_tag: "receipt",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &arguments,
                            }).await.expect("receipt consumer");
                            let receipt = consumer.recv().await.expect("consumer active")
                                .expect("receipt delivery");
                            let confirms = channel.confirm().select().await.expect("confirm.select");
                            let props = Properties::default();
                            let mut first = Pinned::new(confirms.publish(
                                common::client::channel::request("", "first", &props, b"first"),
                            ));
                            assert!(poll_once(first.as_pin()).await.is_pending());
                            channel.queue().purge("barrier").await.expect("barrier reply");
                            let mut second = Pinned::new(confirms.publish(
                                common::client::channel::request("", "second", &props, b"second"),
                            ));
                            assert!(poll_once(second.as_pin()).await.is_pending());
                            receipt.settle(ack::Disposition::Ack).await.expect("receipt ACK");
                            let first = first.await.expect("first confirmation");
                            let second = second.await.expect("second confirmation");
                            conn.control().close(ready, 200, "done").await.expect("close");
                            assert_eq!(first, protocol::Confirmation::Ack);
                            assert_eq!(second, protocol::Confirmation::Nack { requeue: false });
                        }))
                        .expect("fiber");
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
}

#[test]
fn keeps_incomplete_publish_unconfirmed() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            let channel = common::basic::returned::open_channel(stream).expect("channel");
            confirm_select(stream, channel).expect("confirm.select");
            Method::read(stream)
                .expect("publish method")
                .check(channel, 60, 40)
                .expect("publish route");
            Method::write_parts(stream, channel, 60, 80, &[&0u64.to_be_bytes(), &[1]])
                .expect("acknowledge completed publications");
            Method::write(stream, 0, 10, 60, b"\x05ready").expect("ACK observation barrier");
            let common::amqp::wire::Frame(kind, actual, _) =
                common::amqp::wire::Frame::read(stream).expect("content header");
            assert_eq!((kind, actual), (2, channel));
            let common::amqp::wire::Frame(kind, actual, body) =
                common::amqp::wire::Frame::read(stream).expect("content body");
            assert_eq!(
                (kind, actual, body.as_slice()),
                (3, channel, b"x".as_slice())
            );
            Method::write_parts(stream, channel, 60, 120, &[&1u64.to_be_bytes(), &[0]])
                .expect("reject completed publication");
            common::basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().egress(config::Egress::new(1, 4096)));
            with_config(
                broker.addr(),
                config,
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let confirms = channel.confirm().select().await.expect("confirm.select");
                        let props = Properties::default();
                        let mut publish = Pinned::new(confirms.publish(
                            common::client::channel::request("", "incomplete", &props, b"x"),
                        ));
                        // The sole regular frame admits only basic.publish on this poll.
                        assert!(poll_once(publish.as_pin()).await.is_pending());
                        while !conn.observe().read(|view| view.is_blocked()).await {
                            Yield::new().await;
                        }
                        let outcome = publish.await.expect("publication confirmation");
                        conn.control().close(ready, 200, "done").await.expect("close");
                        assert_eq!(outcome, protocol::Confirmation::Nack { requeue: false });
                    }))
                    .expect("fiber");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("broker");
        },
    )
    .expect("scripted broker");
}

#[test]
fn keeps_first_settlement() {
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
            confirm_select(stream, 1).expect("AMQP fixture");

            check_publish(stream, 1, "first", b"first").expect("AMQP fixture");
            confirm_ack(stream, 1, 1).expect("AMQP fixture");
            let mut dup = Vec::with_capacity(9);
            dup.extend_from_slice(&1_u64.to_be_bytes());
            dup.push(0);
            stream
                .write_all(&Method::try_encode(1, 60, 120, &dup).expect("AMQP frame fixture"))
                .expect("duplicate basic.nack");
            Method::read(stream)
                .expect("settlement barrier")
                .check(1, 50, 30)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0_u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("settlement barrier-ok");

            check_publish(stream, 1, "second", b"second").expect("AMQP fixture");
            confirm_ack(stream, 1, 2).expect("AMQP fixture");
            Method::read(stream)
                .expect("AMQP method")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection.close-ok");
        },
        |broker| {
            with_client(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel open");
                        let confirms = channel.confirm().select().await.expect("confirm.select");
                        let props = Properties::default();
                        let mut first = Pinned::new(confirms.publish(publish::Request {
                            exchange: "",
                            routing_key: "first",
                            mandatory: false,
                            properties: &props,
                            body: b"first",
                        }));
                        assert!(poll_once(first.as_pin()).await.is_pending());
                        channel
                            .queue()
                            .purge("barrier")
                            .await
                            .expect("settlement barrier");
                        let first_outcome = first.await.expect("first confirmation");
                        assert_eq!(first_outcome, protocol::Confirmation::Ack);
                        let second_outcome = confirms
                            .publish(publish::Request {
                                exchange: "",
                                routing_key: "second",
                                mandatory: false,
                                properties: &props,
                                body: b"second",
                            })
                            .await
                            .expect("reused confirm slot");
                        assert_eq!(second_outcome, protocol::Confirmation::Ack);
                        conn
                            .control()
                            .close(ready, 200, "done")
                            .await
                            .expect("close");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("duplicate-confirm broker");
        },
    )
    .expect("duplicate-confirm broker");
}

#[test]
fn stale_drop_keeps_current() {
    for poll_old in [false, true] {
        AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first channel.open-ok");
            confirm_select(&mut first, 1).expect("AMQP fixture");
            check_publish(&mut first, 1, "stale", b"old").expect("AMQP fixture");
            drop(first);

            let mut second = acceptor.accept().expect("recovered connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("replayed channel.open-ok");
            confirm_select(&mut second, 1).expect("AMQP fixture");

            check_publish(&mut second, 1, "current", b"new").expect("AMQP fixture");
            Method::read(&mut second).expect("current publication barrier")
                .check(1, 50, 30).expect("queue.purge");
            Method::write(&mut second, 1, 50, 31, &[0; 4]).expect("current publication observed");
            Method::read(&mut second).expect("stale drop barrier")
                .check(1, 50, 30).expect("queue.purge");
            confirm_ack(&mut second, 1, 1).expect("AMQP fixture");
            Method::write(&mut second, 1, 50, 31, &[0; 4]).expect("stale drop observed");
            assert_eq!(second.read(&mut [0]).expect("confirm probe teardown"), 0);
        },
        |broker| {
            let chan = config::Channel::default().confirms(1);
            let cap = config::Capacity::default().channel(chan);
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);
            with_config(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                let target = conn.observe().ready().await.expect("first ready");
                let channel = conn
                    .channels()
                    .open_in(target, 1)
                    .await
                    .expect("channel open");
                let channel = channel.into_auto();
                let confirms = channel.confirm().select().await.expect("confirm.select");
                let props = Properties::default();
                let mut stale = Pinned::new(confirms.publish(publish::Request {
                    exchange: "",
                    routing_key: "stale",
                    mandatory: false,
                    properties: &props,
                    body: b"old",
                }));
                assert!(poll_once(stale.as_pin()).await.is_pending());

                let retired = conn.observe().retired(target.epoch()).await;
                assert!(retired.is_ok(), "old transport must retire: {retired:?}");
                let ready = conn.observe().ready().await.expect("replayed topology must become ready");
                assert!(ready.epoch() != target.epoch(), "publication must use a new transport epoch");

                let mut current = Pinned::new(confirms.publish(publish::Request {
                    exchange: "",
                    routing_key: "current",
                    mandatory: false,
                    properties: &props,
                    body: b"new",
                }));
                assert!(poll_once(current.as_pin()).await.is_pending());
                channel.queue().purge("current").await.expect("current publication sent");
                if poll_old {
                    assert!(matches!(
                        poll_once(stale.as_pin()).await,
                        std::task::Poll::Ready(Err(failure::Runtime::Local(failure::Error::Closed)))
                    ));
                }
                let (_, alloc) = Tracker::usage(|| drop(stale));
                assert_eq!(alloc, (0, 0));
                channel.queue().purge("dropped").await.expect("stale confirm dropped");
                let outcome = current.await.expect("current confirm");
                assert_eq!(outcome, protocol::Confirmation::Ack);
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("confirm reconnect broker");
        },
    ).expect("confirm reconnect broker");
    }
}

#[test]
fn rejects_single_tag() {
    for tag in [0, 2] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
                Method::read(stream)
                    .expect("AMQP method")
                    .check(1, 20, 10)
                    .expect("AMQP method route");
                Method::write(stream, 1, 20, 11, &[0; 4]).expect("channel.open-ok");
                confirm_select(stream, 1).expect("AMQP fixture");
                check_publish(stream, 1, "first", b"first").expect("AMQP fixture");
                confirm_ack(stream, 1, tag).expect("invalid single ACK");
                while stream.read(&mut [0; 256]).expect("invalid confirmation teardown") != 0 {}
            },
            |broker| {
                with_client(
                    broker.addr(),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel open");
                            let confirms = channel.confirm().select().await.expect("confirm.select");
                            let props = Properties::default();
                            let error = confirms
                                .publish(publish::Request {
                                    exchange: "",
                                    routing_key: "first",
                                    mandatory: false,
                                    properties: &props,
                                    body: b"first",
                                })
                                .await
                                .expect_err("invalid single confirmation must not settle");
                            assert!(matches!(
                                error.error(),
                                failure::Error::Protocol(failure::wire::Fault::ConfirmTag {
                                    tag: actual,
                                    published: 1,
                                    multiple: false,
                                }) if *actual == tag
                            ));
                        }))
                        .expect("AMQP fiber must complete");
                    },
                    recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                broker.finish().expect("invalid single confirm broker");
            },
        )
        .expect("invalid single confirm broker");
    }
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
