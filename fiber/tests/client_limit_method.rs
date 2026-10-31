use std::convert::Infallible;
use std::io::{ErrorKind, Read as _, Write as _};

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::property::Properties;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use std::sync::mpsc;

use dope_amqp091::adapter::publish;
use dope_amqp091::failure;
use dope_test::fibers::Yield;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn drop_releases_capacity() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");

            let first = Method::read(stream).expect("AMQP method");
            first.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&first).expect("AMQP fixture"), "first");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("first purge ok");

            let third = Method::read(stream).expect("AMQP method");
            third.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&third).expect("AMQP fixture"), "third");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("third purge ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default().channel(config::Channel::default().methods(2)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            wait_ready(conn).await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");

                            let mut first = Pinned::new(channel.queue().purge("first"));
                            assert!(poll_once(first.as_pin()).await.is_pending());

                            let mut second = Pinned::new(channel.queue().purge("second"));
                            assert!(poll_once(second.as_pin()).await.is_pending());

                            let result = channel.queue().purge("overflow").await;
                            assert!(matches!(result, Err(ref error) if matches!(error.error(), failure::Error::Capacity(failure::wire::Capacity::Method))));
                            drop(second);

                            let mut third = Pinned::new(channel.queue().purge("third"));
                            assert!(poll_once(third.as_pin()).await.is_pending());

                            first.await.expect("first purge");
                            third.await.expect("third purge");
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
            broker.finish().expect("broker");
        },
    )
    .expect("spawn method-capacity broker");
}

#[test]
fn queues_after_stream() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");
            check_publish(stream, 1, "streamed", &STREAMED_BODY).expect("AMQP fixture");
            let queued = Method::read(stream).expect("AMQP method");
            queued.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&queued).expect("AMQP fixture"), "after-stream");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("queued purge ok");
            check_publish(stream, 1, "small", b"x").expect("AMQP fixture");
            check_publish(stream, 1, "empty", b"").expect("AMQP fixture");
            Method::read(stream)
                .expect("AMQP method")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(config::Capacity::default().egress(config::Egress::new(1, 1024))),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = wait_ready(conn).await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let properties = Properties::default();
                        let mut streamed = Pinned::new(channel.basic().publish(publish::Request {
                            exchange: "",
                            routing_key: "streamed",
                            mandatory: false,
                            properties: &properties,
                            body: &STREAMED_BODY,
                        }));
                        assert!(poll_once(streamed.as_pin()).await.is_pending());
                        let mut queued = Pinned::new(channel.queue().purge("after-stream"));
                        assert!(poll_once(queued.as_pin()).await.is_pending());
                        streamed.await.expect("streamed publish");
                        queued.await.expect("serialized purge");
                        channel
                            .basic()
                            .publish(publish::Request {
                                exchange: "",
                                routing_key: "small",
                                mandatory: false,
                                properties: &properties,
                                body: b"x",
                            })
                            .await
                            .expect("small publish");
                        channel
                            .basic()
                            .publish(publish::Request {
                                exchange: "",
                                routing_key: "empty",
                                mandatory: false,
                                properties: &properties,
                                body: b"",
                            })
                            .await
                            .expect("empty publish");
                        conn.control()
                            .close(ready, 200, "OK")
                            .await
                            .expect("close");
                    })).is_ok(), "AMQP fiber must complete");
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
    .expect("spawn outbound-capacity broker");
}

#[test]
fn drop_quarantines_borrow() {
    let (sent, seen) = mpsc::channel();
    let (closed, close_rx) = mpsc::channel();
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");
            Method::read(stream).expect("AMQP method").check(1, 60, 40).expect("AMQP method route");
            sent.send(()).expect("publish send observation");

            let mut bytes = [0; 4096];
            loop {
                match stream.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => {
                        assert_eq!(error.kind(), ErrorKind::ConnectionReset);
                        break;
                    }
                }
            }
            closed.send(()).expect("publish close observation");
        },
        |broker| {
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().egress(config::Egress::new(2, 1024)));
            with(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                wait_ready(conn).await.expect("ready");
                let channel = conn.channels().open(1).await.expect("open channel");
                let properties = Properties::default();
                let mut body = [7; 2048];
                let mut publishing = Pinned::new(channel.basic().publish(publish::Request {
                    exchange: "",
                    routing_key: "cancelled",
                    mandatory: false,
                    properties: &properties,
                    body: &body,
                }));
                assert!(poll_once(publishing.as_pin()).await.is_pending());
                loop {
                    match seen.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        disconnected => { disconnected.expect("broker ended before observing publish"); }
                    }
                }
                drop(publishing);
                body.fill(0);

                loop {
                    match close_rx.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        disconnected => { disconnected.expect("broker ended before publishing close observation"); }
                    }
                }
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("publish cancellation broker");
        },
    )
    .expect("spawn publish-cancellation broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
