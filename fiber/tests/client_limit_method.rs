use dope_test::harness::peer;
use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::driver::schedule::ready::wake::queue;
use dope::manifold::{client::close, timing};
use dope_amqp091::config;
use dope_amqp091::message::property::Properties;

use crate::common::amqp::Peer;
use crate::common::amqp::app::{Endpoint, Policy};
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use dope_amqp091::adapter::publish;
use dope_amqp091::failure;
use dope_test::fibers::Yield;
use dope_test::harness::peer::{AcceptScript, TcpScript};

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
                            conn.observe().ready().await.expect("ready");
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
                        let ready = conn.observe().ready().await.expect("ready");
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
    AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            let stream = &mut first;
            stream
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            stream
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
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
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 40)
                .expect("AMQP method route");
            Method::write(stream, 0, 10, 60, b"\x04sent")
                .expect("publish observed before cancellation");

            let mut bytes = [0; 4096];
            loop {
                match stream.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => {
                        assert!(matches!(error, peer::Error::Syscall(error) if error.is_reset()));
                        break;
                    }
                }
            }
            drop(first);

            let mut second = acceptor.accept().expect("reconnected transport");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("reconnected handshake");
            Method::read(&mut second)
                .expect("replayed channel")
                .check(1, 20, 10)
                .expect("Channel.Open");
            Method::write(&mut second, 1, 20, 11, &[0; 4]).expect("replayed channel open ok");
            assert_eq!(second.read(&mut [0]).expect("endpoint teardown"), 0);
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
                let ready = conn.observe().ready().await.expect("ready");
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
                while !conn.observe().read(|view| view.is_blocked()).await {
                    Yield::new().await;
                }
                drop(publishing);
                body.fill(0);

                conn.observe().retired(ready.epoch()).await.expect("cancelled publish retirement");
                conn.observe().ready().await.expect("reconnected ready");
                conn.observe().read(|view| {
                    assert_eq!(view.generation(), 2);
                    let cause = view.last_exit().expect("publish cancellation cause");
                    assert_eq!(cause.generation(), 1);
                    assert!(matches!(cause.error(), failure::Error::Disconnected(
                        failure::Reason::Local(close::LocalOrigin::OperationCancelled)
                    )));
                }).await;
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
            broker.finish().expect("publish cancellation broker");
        },
    )
    .expect("spawn publish-cancellation broker");
}

#[test]
fn abort_discards_partial_publish() {
    AcceptScript::with(
        |acceptor| {
            let mut stream = acceptor.accept().expect("connection");
            stream
                .read_wait(Some(Duration::from_secs(10)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(10)))
                .expect("write timeout");
            Peer::new(&mut stream)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut stream)
                .expect("channel open")
                .check(1, 20, 10)
                .expect("Channel.Open");
            Method::write(&mut stream, 1, 20, 11, &[0; 4]).expect("channel open ok");
            match stream.read(&mut [0]) {
                Ok(read) => assert_eq!(read, 0, "aborted publication reached the wire"),
                Err(error) => {
                    assert!(matches!(error, peer::Error::Syscall(error) if error.is_reset()));
                }
            }
            drop(stream);
            assert_eq!(
                Redial::observe(acceptor).expect("redial observation"),
                Redial::Elapsed
            );
        },
        |broker| {
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().egress(config::Egress::new(1, 1024)));
            Endpoint::new(broker.addr(), cfg)
                .shutdown(broker.completion())
                .try_check::<timing::Throughput, _>(
                    Policy::new(1, 0, Duration::from_secs(10)),
                    |rt| {
                        let conn = rt.connection();
                        rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                            while !conn.observe().read(|view| view.is_ready()).await {
                                Yield::new().await;
                            }
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open_in(ready, 1).await.expect("channel");
                            let properties = Properties::default();
                            let result = channel.basic().publish(publish::Request {
                                exchange: "",
                                routing_key: "aborted",
                                mandatory: false,
                                properties: &properties,
                                body: b"body",
                            }).await;
                            assert!(matches!(result, Err(failure::Runtime::Local(
                                failure::Error::Queue(queue::Error::Full { .. })
                            ))));
                            while !conn.observe().read(|view| view.is_failed()).await {
                                Yield::new().await;
                            }
                            let error = conn.observe().ready().await.expect_err("permanent abort");
                            let failure::Runtime::Terminal(cause) = error else {
                                panic!("missing terminal cause");
                            };
                            assert_eq!(cause.generation(), 1);
                            assert!(matches!(cause.error(), failure::Error::Disconnected(
                                failure::Reason::Protocol
                            )));
                        }))
                        .expect("broker completion")
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
            broker.finish().expect("publish abort broker");
        },
    )
    .expect("spawn publish-abort broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
