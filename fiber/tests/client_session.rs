use std::convert::Infallible;
use std::io::Write as _;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short};

pub mod common;

use std::sync::mpsc;

use dope_amqp091::failure;
use dope_amqp091::message::ack;
use dope_test::harness::peer::AcceptScript;

use crate::common::client::*;

fn assert_auto_consume(method: &Method) {
    method.check(1, 60, 20).expect("AMQP method route");
    let args = method.arguments();
    let tag = 3 + usize::from(args[2]);
    let flags = tag + 1 + usize::from(args[tag]);
    assert_eq!(args[flags], 0b10, "auto ACK bit");
}

#[test]
fn replays_after_retry() {
    let (step, steps) = mpsc::channel();
    AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(5)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(5)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first Channel.OpenOk");
            let consume = Method::read(&mut first).expect("AMQP method");
            assert_auto_consume(&consume);
            let mut ok = Vec::new();
            put_short(&mut ok, b"t").expect("AMQP short string");
            first
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("first Basic.ConsumeOk");
            steps.recv().expect("first consumer readiness");
            drop(first);

            let mut retry = acceptor.accept().expect("retry connection");
            retry
                .read_wait(Some(Duration::from_secs(5)))
                .expect("retry read timeout");
            retry
                .write_wait(Some(Duration::from_secs(5)))
                .expect("retry write timeout");
            Peer::new(&mut retry)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut retry)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            drop(retry);

            let mut third = acceptor.accept().expect("third connection");
            third
                .read_wait(Some(Duration::from_secs(5)))
                .expect("third read timeout");
            third
                .write_wait(Some(Duration::from_secs(5)))
                .expect("third write timeout");
            Peer::new(&mut third)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut third)
                .expect("replayed channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            third
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("replayed Channel.OpenOk");
            let consume = Method::read(&mut third).expect("replayed basic.consume");
            assert_auto_consume(&consume);
            let mut ok = Vec::new();
            put_short(&mut ok, b"t").expect("AMQP short string");
            third
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("replayed Basic.ConsumeOk");
            steps.recv().expect("third transport verification");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let first = conn.observe().ready().await.expect("first ready");
                            let chan = conn
                                .channels()
                                .open_in(first, 1)
                                .await
                                .expect("open channel");
                            let empty = Table::new();
                            let consumer = chan
                                .basic().consume(basic::Consume {
                                    queue: "q",
                                    consumer_tag: "t",
                                    no_local: false,
                                    ack: ack::Auto,
                                    exclusive: false,
                                    arguments: &empty,
                                })
                                .await
                                .expect("basic_consume");
                            step.send(()).expect("publish first consumer readiness");
                            conn.observe()
                                .retired(first.epoch())
                                .await
                                .expect("first transport retirement");
                            conn.observe()
                                .ready()
                                .await
                                .expect("third transport recovery");
                            let generation = conn.observe().read(|view| view.ready_cycle()).await;
                            assert_eq!(generation, Some(3));
                            step.send(()).expect("publish third transport verification");
                            drop(consumer);
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
            broker.finish().expect("broker thread panicked");
        },
    )
    .expect("bind broker");
}

#[test]
fn rejects_stale_ready() {
    let (ready, wait) = mpsc::channel();
    AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("first AMQP handshake");
            wait.recv().expect("first connection readiness");
            drop(first);

            let mut current = acceptor.accept().expect("recovered connection");
            current
                .read_wait(Some(Duration::from_secs(5)))
                .expect("recovered read timeout");
            current
                .write_wait(Some(Duration::from_secs(5)))
                .expect("recovered write timeout");
            Peer::new(&mut current)
                .open(TUNE)
                .expect("recovered AMQP handshake");
            let close = Method::read(&mut current).expect("current connection.close");
            close.check(0, 10, 50).expect("AMQP method route");
            let args = close.arguments();
            let len = usize::from(args[2]);
            assert_eq!(&args[3..3 + len], b"current");
            current
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("current connection.close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let stale = conn.observe().ready().await.expect("first ready");
                            ready.send(()).expect("publish first connection readiness");
                            conn
                                .observe()
                                .retired(stale.epoch())
                                .await
                                .expect("first retirement");
                            let current = conn
                                .observe()
                                .ready()
                                .await
                                .expect("recovered ready");
                            assert!(stale.epoch() != current.epoch());
                            let rejected = conn.control().start(stale, 200, "stale").await;
                            assert!(matches!(rejected, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                            conn
                                .control()
                                .close(current, 200, "current")
                                .await
                                .expect("close recovered connection");
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
            broker.finish().expect("stale-ready broker");
        },
    )
    .expect("bind stale-ready broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
