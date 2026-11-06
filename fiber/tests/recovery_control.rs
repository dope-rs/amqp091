use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::wire::{Frame, Method};
use crate::common::amqp::{Greeting, Peer};
pub mod common;
use std::time::Duration;

use dope_amqp091::failure;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_test::harness::peer::AcceptScript;

use crate::common::basic::delivery;
use crate::common::recovery::TUNE;
use crate::common::recovery::client::with;

#[test]
fn answers_empty_challenge() {
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept().expect("connection");
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .expect("write timeout");
            let nonce = b"";
            Peer::new(&mut stream)
                .preface()
                .expect("AMQP preface")
                .start(Greeting::PLAIN)
                .expect("Connection.Start")
                .recv()
                .expect("Connection.StartOk")
                .secure(nonce)
                .expect("Connection.Secure")
                .recv(b"\0guest\0guest")
                .expect("Connection.SecureOk")
                .tune(TUNE)
                .expect("Connection.Tune")
                .recv()
                .expect("Connection.TuneOk")
                .recv()
                .expect("Connection.Open")
                .send()
                .expect("Connection.OpenOk");
            assert_eq!(stream.read(&mut [0]).expect("endpoint teardown"), 0);
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(conn.observe().epoch())
                        .expect("challenge drive")
                        .expect("challenge");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn rejects_challenge() {
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept().expect("connection");
            assert!(stream.read_wait(Some(Duration::from_secs(5))).is_ok());
            assert!(stream.write_wait(Some(Duration::from_secs(5))).is_ok());
            let opening = Peer::new(&mut stream)
                .preface()
                .and_then(|peer| peer.start(Greeting::PLAIN))
                .and_then(|peer| peer.recv())
                .and_then(|peer| peer.secure(b"nonce"));
            assert!(opening.is_ok(), "send nonempty challenge");
            drop(opening);
            assert!(
                matches!(stream.read(&mut [0]), Ok(0)),
                "reject without credential response"
            );
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    let error = rt
                        .block_on(conn.observe().epoch())
                        .expect("fiber drive")
                        .expect_err("nonempty challenge must fail");
                    assert!(matches!(error.error(), failure::Error::Protocol(
                failure::wire::Fault::Challenge { actual }
            ) if actual.get() == b"nonce".len()));
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            assert!(broker.finish().is_ok(), "scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn times_out_startup_before_ready() {
    use dope::manifold::timing::Policy as _;
    use dope_amqp091::config;
    use dope_fiber::task::time::sleep::TimerExt as _;

    use common::amqp::app::Endpoint;
    use common::recovery::{Startup, client::POLICY};

    for open in [false, true] {
        AcceptScript::with(
            move |acceptor| {
                let mut first = acceptor.accept().expect("first connection");
                let preface = Peer::new(&mut first).preface().expect("AMQP preface");
                if open {
                    preface
                        .start(Greeting::PLAIN)
                        .expect("Connection.Start")
                        .recv()
                        .expect("Connection.StartOk")
                        .tune(TUNE)
                        .expect("Connection.Tune")
                        .recv()
                        .expect("Connection.TuneOk")
                        .recv()
                        .expect("Connection.Open");
                }
                assert_eq!(first.read(&mut [0]).expect("startup timeout close"), 0);
                drop(first);

                let mut second = acceptor.accept().expect("startup reconnect");
                Peer::new(&mut second)
                    .open(TUNE)
                    .expect("AMQP broker handshake");
                assert_eq!(second.read(&mut [0]).expect("endpoint teardown"), 0);
            },
            |broker| {
                let config = config::Config::new("guest", "guest")
                    .expect("AMQP config")
                    .tune(config::Tune::default().heartbeat(0));
                Endpoint::new(broker.addr(), config).try_check::<Startup, _>(
                    POLICY,
                    |rt| {
                        let timer = rt.timer();
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("startup recovery");
                            conn.observe().read(|view| {
                                let cause = view.last_exit().expect("stalled startup cause");
                                assert_eq!(cause.reason(), failure::Reason::InboundTimeout);
                            }).await;
                            timer
                                .sleep(Startup::IDLE_WINDOW.get() * 2)
                                .expect("ready idle wait")
                                .await;
                            conn.observe().read(|view| {
                                assert!(view.ready() == Some(ready));
                            }).await;
                        }))
                        .expect("startup timeout drive");
                    },
                    recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                broker.finish().expect("scripted broker thread");
            },
        )
        .expect("scripted broker");
    }
}

#[test]
fn discards_during_close() {
    for short in [false, true] {
        AcceptScript::with(
            move |acceptor| {
                let mut stream = acceptor.accept().expect("connection");
                stream
                    .read_wait(Some(Duration::from_secs(5)))
                    .expect("read timeout");
                stream
                    .write_wait(Some(Duration::from_secs(5)))
                    .expect("write timeout");
                Peer::new(&mut stream)
                    .open(TUNE)
                    .expect("AMQP broker handshake");
                Method::read(&mut stream)
                    .expect("Channel.Open")
                    .check(1, 20, 10)
                    .expect("AMQP method route");
                Method::write(&mut stream, 1, 20, 11, &[0, 0, 0, 0])
                    .expect("Channel.OpenOk");
                delivery::check_consume::<ack::Manual>(
                    &Method::read(&mut stream).expect("Basic.Consume"),
                    1,
                    "q",
                    "closing",
                )
                .expect("AMQP consumer");
                Method::write(
                    &mut stream,
                    1,
                    60,
                    21,
                    &delivery::consume_ok("closing").expect("consumer tag"),
                )
                .expect("Basic.ConsumeOk");
                Method::read(&mut stream)
                    .expect("Connection.Close")
                    .check(0, 10, 50)
                    .expect("AMQP method route");
                let mut frames = if short {
                    Frame::try_encode(1, 1, &[0, 60, 0]).expect("short method")
                } else {
                    delivery::byte("closing", 1, b'x').expect("in-flight delivery")
                };
                frames.extend_from_slice(
                    &Method::try_encode(0, 10, 60, b"\x07ignored").expect("AMQP frame fixture"),
                );
                frames.extend_from_slice(
                    &Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"),
                );
                stream.write_all(&frames).expect("traffic during close");
            },
            |broker| {
                with(
                    broker.addr(),
                    |rt| {
                        let conn = rt.connection();
                        let result = rt
                            .block_on(dope_fiber::fiber!('_ => async move {
                                let ready = conn.observe().ready().await.expect("ready");
                                let channel = conn.channels().open_in(ready, 1).await.expect("channel");
                                let args = Table::new();
                                let _input = channel.basic().consume(basic::Consume {
                                    queue: "q",
                                    consumer_tag: "closing",
                                    no_local: false,
                                    ack: ack::Manual,
                                    exclusive: false,
                                    arguments: &args,
                                }).await.expect("consumer");
                                conn.control().close(ready, 200, "contract complete").await
                            }))
                            .expect("fiber drive");
                        if short {
                            assert!(matches!(result, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        } else {
                            result.expect("AMQP operation must succeed");
                        }
                    },
                    recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                broker.finish().expect("scripted broker thread");
            },
        )
        .expect("scripted broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
