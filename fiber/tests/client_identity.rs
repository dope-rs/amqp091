use std::convert::Infallible;
use std::io::Write;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{basic, queue};

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short};

pub mod common;

use std::sync::mpsc;

use dope_amqp091::message::ack;
use dope_test::fibers::Yield;
use dope_test::harness::peer::AcceptScript;

use crate::common::client::*;

#[test]
fn keeps_queue_identities() {
    let (replayed, replay) = mpsc::channel();
    AcceptScript::with(
        move |acceptor| {
            fn declare_ok(mut stream: impl Write, name: &[u8]) {
                stream
                    .write_all(&queue_declare_ok_frame(1, name).expect("AMQP fixture"))
                    .expect("queue.declare-ok");
            }

            fn consume_ok(mut stream: impl Write, tag: &[u8]) {
                let mut response = Vec::new();
                put_short(&mut response, tag).expect("AMQP short string");
                stream
                    .write_all(
                        &Method::try_encode(1, 60, 21, &response).expect("AMQP frame fixture"),
                    )
                    .expect("basic.consume-ok");
            }

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
                .expect("channel.open-ok");

            let declare = Method::read(&mut first).expect("AMQP method");
            declare.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&declare).expect("AMQP fixture"), "");
            declare_ok(&mut first, b"old-a");
            check_queue_bind_target(&Method::read(&mut first).expect("AMQP method"), "", "")
                .expect("AMQP fixture");
            first
                .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("first queue.bind-ok");
            check_consume_target(
                &Method::read(&mut first).expect("AMQP method"),
                "",
                "consumer-a",
            )
            .expect("AMQP fixture");
            consume_ok(&mut first, b"consumer-a");

            let declare = Method::read(&mut first).expect("AMQP method");
            declare.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&declare).expect("AMQP fixture"), "");
            declare_ok(&mut first, b"old-b");
            check_queue_bind_target(&Method::read(&mut first).expect("AMQP method"), "", "")
                .expect("AMQP fixture");
            first
                .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("second queue.bind-ok");
            check_consume_target(
                &Method::read(&mut first).expect("AMQP method"),
                "",
                "consumer-b",
            )
            .expect("AMQP fixture");
            consume_ok(&mut first, b"consumer-b");
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
                .expect("recovered channel.open-ok");

            let replay_a = Method::read(&mut second).expect("AMQP method");
            replay_a.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&replay_a).expect("AMQP fixture"), "");
            declare_ok(&mut second, b"new-a");
            let replay_b = Method::read(&mut second).expect("AMQP method");
            replay_b.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&replay_b).expect("AMQP fixture"), "");
            declare_ok(&mut second, b"new-b");

            check_queue_bind_target(
                &Method::read(&mut second).expect("AMQP method"),
                "new-a",
                "new-a",
            )
            .expect("AMQP fixture");
            second
                .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("replayed first queue.bind-ok");
            check_queue_bind_target(
                &Method::read(&mut second).expect("AMQP method"),
                "new-b",
                "new-b",
            )
            .expect("AMQP fixture");
            second
                .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("replayed second queue.bind-ok");

            check_consume_target(
                &Method::read(&mut second).expect("AMQP method"),
                "new-a",
                "consumer-a",
            )
            .expect("AMQP fixture");
            consume_ok(&mut second, b"consumer-a");
            check_consume_target(
                &Method::read(&mut second).expect("AMQP method"),
                "new-b",
                "consumer-b",
            )
            .expect("AMQP fixture");
            consume_ok(&mut second, b"consumer-b");
            replayed.send(()).expect("publish queue identity replay");
            Method::read(&mut second)
                .expect("connection close")
                .check(0, 10, 50).expect("AMQP method route");
            second
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default().topology(config::Topology::default().identities(2)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let channel = conn
                            .channels()
                            .open_in(target, 1)
                            .await
                            .expect("channel open");
                        let args = Table::new();

                        channel
                            .queue()
                            .declare(queue::Declare {
                                queue: "",
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &args,
                            })
                            .await
                            .expect("first anonymous queue");
                        channel
                            .queue()
                            .bind("", "x", "", &args)
                            .await
                            .expect("first current-queue binding");
                        let first = channel
                            .basic()
                            .consume(basic::Consume {
                                queue: "",
                                consumer_tag: "consumer-a",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("first current-queue consumer");

                        channel
                            .queue()
                            .declare(queue::Declare {
                                queue: "",
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &args,
                            })
                            .await
                            .expect("second anonymous queue");
                        channel
                            .queue()
                            .bind("", "x", "", &args)
                            .await
                            .expect("second current-queue binding");
                        let second = channel
                            .basic()
                            .consume(basic::Consume {
                                queue: "",
                                consumer_tag: "consumer-b",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("second current-queue consumer");

                        loop {
                            match replay.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before queue identity replay"), }
                        }
                        let ready = conn.observe().ready().await.expect("recovered ready");
                        conn.control()
                            .close(ready, 200, "done")
                            .await
                            .expect("connection close");
                        drop(first);
                        drop(second);
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("multi anonymous queue broker");
        },
    )
    .expect("multi anonymous queue broker");
}

#[test]
fn restores_checked_slot() {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (replayed, replay) = mpsc::channel();
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
            let initial = Method::read(&mut first).expect("AMQP method");
            initial.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&initial).expect("AMQP fixture"), "");
            first
                .write_all(&queue_declare_ok_frame(1, b"old").expect("AMQP fixture"))
                .expect("initial queue.declare-ok");
            check_queue_bind_target(&Method::read(&mut first).expect("AMQP method"), "", "")
                .expect("AMQP fixture");
            first
                .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("initial queue.bind-ok");
            ready_rx
                .recv()
                .expect("client committed initial queue binding");
            drop(first);

            let mut middle = acceptor.accept().expect("interrupted recovery");
            middle
                .read_wait(Some(Duration::from_secs(10)))
                .expect("interrupted read timeout");
            middle
                .write_wait(Some(Duration::from_secs(10)))
                .expect("interrupted write timeout");
            Peer::new(&mut middle)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut middle)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            middle
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("interrupted channel.open-ok");
            let claim = Method::read(&mut middle).expect("AMQP method");
            claim.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&claim).expect("AMQP fixture"), "");
            drop(middle);

            let mut last = acceptor.accept().expect("final recovery");
            last.read_wait(Some(Duration::from_secs(10)))
                .expect("recovered read timeout");
            last.write_wait(Some(Duration::from_secs(10)))
                .expect("recovered write timeout");
            Peer::new(&mut last)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut last)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            last.write_all(
                &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
            )
            .expect("recovered channel.open-ok");
            let replay = Method::read(&mut last).expect("AMQP method");
            replay.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&replay).expect("AMQP fixture"), "");
            last.write_all(&queue_declare_ok_frame(1, b"new").expect("AMQP fixture"))
                .expect("recovered queue.declare-ok");
            check_queue_bind_target(
                &Method::read(&mut last).expect("AMQP method"),
                "new",
                "new",
            )
            .expect("AMQP fixture");
            last.write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("recovered queue.bind-ok");
            replayed.send(()).expect("publish identity replay");
            Method::read(&mut last)
                .expect("connection close")
                .check(0, 10, 50).expect("AMQP method route");
            last.write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default().topology(config::Topology::default().identities(1)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let channel = conn
                            .channels()
                            .open_in(target, 1)
                            .await
                            .expect("channel open");
                        let args = Table::new();
                        channel
                            .queue()
                            .declare(queue::Declare {
                                queue: "",
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &args,
                            })
                            .await
                            .expect("anonymous queue");
                        channel
                            .queue()
                            .bind("", "x", "", &args)
                            .await
                            .expect("current-queue binding");
                        ready_tx.send(()).expect("publish initial queue binding");
                        loop {
                            match replay.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before identity replay"), }
                        }
                        let ready = conn.observe().ready().await.expect("recovered ready");
                        conn.control()
                            .close(ready, 200, "done")
                            .await
                            .expect("connection close");
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("identity rollback broker");
        },
    )
    .expect("identity rollback broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
