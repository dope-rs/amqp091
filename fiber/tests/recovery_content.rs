use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Frame, Method};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_test::harness::peer;
use std::time::Duration;

use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{basic, queue};
use dope_test::harness::peer::TcpScript;

use crate::common::recovery::TUNE;
use crate::common::recovery::client::with;

#[test]
fn rejects_partial_body_credit_cycle() {
    use crate::common::amqp::app::Endpoint;
    use crate::common::recovery::{broker::open_named, client::POLICY};
    use dope::manifold::timing;
    use dope_amqp091::{config, failure};

    TcpScript::with(
        move |stream| {
            stream.read_wait(Some(Duration::from_secs(5))).expect("read timeout");
            stream.write_wait(Some(Duration::from_secs(5))).expect("write timeout");
            Peer::new(stream).open(TUNE).expect("handshake");
            for channel in 1..=2 {
                open_named(stream, channel, "q", "tag").expect("consumer");
            }
            Method::read(stream).expect("consumers ready")
                .check(2, 50, 30).expect("queue.purge");
            let mut frames = Vec::new();
            for (channel, size) in [(1, 3), (2, 4)] {
                frames.extend_from_slice(&common::basic::delivery::method(channel, "tag", 1).expect("delivery"));
                frames.extend_from_slice(&common::basic::content_header(channel, size).expect("content header"));
            }
            frames.extend_from_slice(&Frame::try_encode(3, 1, b"abc").expect("first body"));
            frames.extend_from_slice(&Frame::try_encode(3, 2, b"defg").expect("second body"));
            stream.write_all(&frames).expect("interleaved content");
            let read = stream.read(&mut [0]);
            assert!(matches!(read, Ok(0)) || matches!(read,
                Err(ref error) if matches!(error, peer::Error::Syscall(error) if error.is_reset() || error.is_aborted())
            ), "unreleasable body credit must retire the transport: {read:?}");
        },
        |broker| {
            let config = config::Config::new("guest", "guest").expect("config")
                .capacity(config::Capacity::default().body(config::Body::default().buffer(6)));
            Endpoint::new(broker.addr(), config).shutdown(broker.completion()).try_check::<timing::Throughput, _, _>(POLICY, |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("ready");
                    let args = Table::new();
                    let mut consumers = Vec::with_capacity(2);
                    for id in 1..=2 {
                        let channel = conn.channels().open_in(ready, id).await.expect("channel");
                        channel.queue().declare(queue::Declare {
                            queue: "q", durable: false, exclusive: false, auto_delete: false, arguments: &args,
                        }).await.expect("queue");
                        consumers.push(channel.basic().consume(basic::Consume {
                            queue: "q", consumer_tag: "tag", no_local: false, ack: ack::Manual, exclusive: false, arguments: &args,
                        }).await.expect("consumer"));
                        if id == 2 {
                            channel.queue().purge("q").await.expect_err("partial-body credit retires transport");
                        }
                    }
                    let Err(error) = consumers[1].recv().await else {
                        panic!("partial bodies cannot free each other's credit");
                    };
                    let failure::Error::Capacity(failure::wire::Capacity::BodyBudget(credit)) = error.error() else {
                        panic!("body credit failure changed: {error:?}");
                    };
                    let (need, spare) = credit.into_parts();
                    assert_eq!((need.get(), spare), (4, 3));
                })).expect("peer shutdown").expect("body credit failure completes");
            }, recover, complete);
            broker.finish().expect("scripted broker");
        },
    ).expect("body credit peer");
}

#[test]
fn rejects_early_method() {
    TcpScript::with(
        move |stream| {
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .expect("write timeout");
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("Channel.OpenOk");
            Method::read(stream)
                .expect("channel ready")
                .check(1, 50, 30)
                .expect("queue.purge");

            stream
                .write_all(
                    &common::basic::delivery::method(1, "tag", 1).expect("AMQP frame fixture"),
                )
                .expect("Basic.Deliver");
            stream
                .write_all(&common::basic::content_header(1, 10).expect("AMQP frame fixture"))
                .expect("content header");
            stream
                .write_all(&Frame::try_encode(3, 1, b"12345").expect("AMQP frame fixture"))
                .expect("partial body");
            stream
                .write_all(&Method::try_encode(1, 20, 20, &[1]).expect("AMQP frame fixture"))
                .expect("interleaved Channel.Flow");
            let close = Method::read(stream).expect("Connection.Close");
            close.check(0, 10, 50).expect("AMQP method route");
            let code = close.1[4..6]
                .try_into()
                .expect("connection close code occupies two octets");
            assert_eq!(u16::from_be_bytes(code), 505);
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("Connection.CloseOk");
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            channel.queue().purge("").await.expect_err("content violation");
                            conn.observe().retired(ready.epoch()).await.expect("content violation retirement");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn accepts_empty_fragment() {
    TcpScript::with(
        move |stream| {
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .expect("write timeout");
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("Channel.OpenOk");
            Method::read(stream)
                .expect("Queue.Declare")
                .check(1, 50, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &common::recovery::broker::queue_declare_ok(1, "q", 0, 0)
                        .expect("AMQP frame fixture"),
                )
                .expect("Queue.DeclareOk");
            Method::read(stream)
                .expect("Basic.Consume")
                .check(1, 60, 20)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 60, 21, &[3, b't', b'a', b'g'])
                        .expect("AMQP frame fixture"),
                )
                .expect("Basic.ConsumeOk");

            stream
                .write_all(
                    &common::basic::delivery::method(1, "tag", 1).expect("AMQP frame fixture"),
                )
                .expect("Basic.Deliver");
            stream
                .write_all(&common::basic::content_header(1, 5).expect("AMQP frame fixture"))
                .expect("content header");
            stream
                .write_all(&Frame::try_encode(3, 1, &[]).expect("AMQP frame fixture"))
                .expect("empty content fragment");
            stream
                .write_all(&Frame::try_encode(3, 1, b"hello").expect("AMQP frame fixture"))
                .expect("content body");
            Method::read(stream)
                .expect("connection close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            let args = Table::new();
                            channel
                                .queue()
                                .declare(queue::Declare {
                                    queue: "q",
                                    durable: false,
                                    exclusive: false,
                                    auto_delete: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("queue");
                            let mut consumer = channel
                                .basic()
                                .consume(basic::Consume {
                                    queue: "q",
                                    consumer_tag: "tag",
                                    no_local: false,
                                    ack: ack::Manual,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("consumer");
                            let delivery = consumer
                                .recv()
                                .await
                                .expect("delivery state")
                                .expect("delivery");
                            assert!(delivery.body() == b"hello"[..]);
                            conn.control()
                                .close(ready, 200, "done")
                                .await
                                .expect("connection close");
                            delivery.release();
                            drop(consumer);
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}
