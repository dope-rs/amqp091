use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope::manifold::timing;
use dope_amqp091::{config, protocol};
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::{Frame, Method};

pub mod common;

use common::client::Redial;
use common::client::broker::{close, handshake, illegal};
use dope_amqp091::failure;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn rejects_scoped_method() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");
            stream
                .write_all(&Method::try_encode(1, 10, 60, b"\x03bad").expect("AMQP frame fixture"))
                .expect("wrong-scope Connection.Blocked");
            let close = Method::read(stream).expect("AMQP method");
            close.check(0, 10, 50).expect("AMQP method route");
            let code = close.1[4..6]
                .try_into()
                .expect("reply code occupies two octets");
            assert_eq!(u16::from_be_bytes(code), 504);
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let result = channel.queue().purge("never").await;
                            assert!(matches!(result, Err(ref error) if matches!(error.error(), failure::Error::Protocol(failure::wire::Fault::ConnectionChannel {
                                    channel: 1
                                }))));
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
    .expect("spawn method-scope broker");
}

#[test]
fn rejects_heartbeat() {
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");
            let trigger = Method::read(stream).expect("AMQP method");
            trigger.check(1, 50, 30).expect("AMQP method route");
            stream
                .write_all(&Frame::try_encode(8, 0, b"x").expect("AMQP frame fixture"))
                .expect("malformed heartbeat");
            while stream.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let result = channel.queue().purge("trigger").await;
                            assert!(matches!(result, Err(ref error) if matches!(error.error(), failure::Error::Frame(protocol::wire::frame::Fault::HeartbeatPayload {
                                    len: 1
                                }))));
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
    .expect("spawn malformed-heartbeat broker");
}

#[test]
fn rejects_unknown_channel() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            stream
                .write_all(&Method::try_encode(7, 20, 20, &[1]).expect("AMQP frame fixture"))
                .expect("flow on unknown channel");
            let close = Method::read(stream).expect("AMQP method");
            close.check(0, 10, 50).expect("AMQP method route");
            let code = close.1[4..6]
                .try_into()
                .expect("reply code occupies two octets");
            assert_eq!(u16::from_be_bytes(code), 504);
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let result = match conn.observe().epoch().await {
                                Ok(epoch) => {
                                    conn.observe().retired(epoch).await.expect("unknown-channel retirement");
                                    conn.observe().epoch().await
                                }
                                failure => failure,
                            };
                            assert!(matches!(result, Err(ref error) if matches!(error.error(), failure::Error::Protocol(failure::wire::Fault::ChannelUnknown {
                                    channel: 7
                                }))), "unknown-channel observation: {result:?}");
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
            broker.finish().expect("unknown channel flow broker");
        },
    )
    .expect("spawn unknown-channel broker");
}

#[test]
fn rejects_server_version() {
    handshake((1, 0), b"en_US", |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let handshake = conn.observe().epoch().await;
                    assert!(matches!(handshake, Err(ref error) if matches!(error.error(), failure::Error::Version { major: 1, minor: 0 })));
                    let failed = conn.observe().read(|view| view.is_failed()).await;
                    assert!(failed);
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
        assert!(
            broker
                .finish()
                .expect("fatal handshake broker")
                .expect("broker protocol")
                == Redial::Elapsed,
            "connector redialed after an unsupported AMQP version"
        );
    })
    .expect("bind broker");
}

#[test]
fn rejects_coalesced_handshake() {
    for partial in [false, true] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).preface().expect("client preface");
                let mut frames =
                    Method::try_encode(0, 10, 10, b"\0\x09\0\0\0\0\0\0\0\x05PLAIN\0\0\0\x05en_US")
                        .expect("start");
                let prefix = frames.len() + 2;
                frames.extend_from_slice(
                    &Method::try_encode(0, 10, 30, b"\0\x10\0\x02\0\0\0\0").expect("tune"),
                );
                frames.extend_from_slice(&Method::try_encode(0, 10, 41, &[0]).expect("open ok"));
                let end = if partial { prefix } else { frames.len() };
                stream
                    .write_all(&frames[..end])
                    .expect("coalesced handshake");
                if partial {
                    Method::read(stream)
                        .expect("start ok")
                        .check(0, 10, 11)
                        .expect("start ok route");
                    stream.write_all(&frames[end..]).expect("handshake tail");
                }
                let _ = stream.read(&mut [0; 128]);
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest").expect("config"),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let result = conn.observe().epoch().await;
                            assert!(
                                matches!(result, Err(ref error) if matches!(
                                    error.error(),
                                    failure::Error::Protocol(failure::wire::Fault::ConnMethod {
                                        method: failure::wire::ConnMethod::Tune,
                                        phase: failure::wire::ConnPhase::Start,
                                        closing: false,
                                    })
                                )),
                                "premature Tune: {result:?}"
                            );
                        }))
                        .expect("AMQP fiber");
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
        .expect("spawn handshake broker");
    }
}

#[test]
fn rejects_retained_method() {
    illegal(0, 10, 60, &[1, b'x'], |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let handshake = conn.observe().epoch().await;
                    assert!(matches!(handshake, Err(ref error) if matches!(error.error(), failure::Error::Protocol(failure::wire::Fault::ConnMethod {
                            method: failure::wire::ConnMethod::Blocked,
                            phase: failure::wire::ConnPhase::Start,
                            closing: false,
                        }))));
                    let failed = conn.observe().read(|view| view.is_failed()).await;
                    assert!(failed);
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
        let (sent, closed, redial) = broker
            .finish()
            .expect("illegal handshake broker")
            .expect("broker protocol");
        assert!(!sent, "client emitted a method after failed admission");
        assert!(closed, "client kept the invalid transport alive");
        assert_eq!(
            redial,
            Redial::Elapsed,
            "client redialed after a fatal phase violation"
        );
    })
    .expect("bind broker");
}

#[test]
fn rejects_handshake_flow() {
    illegal(1, 20, 20, &[1], |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let handshake = conn.observe().epoch().await;
                    assert!(matches!(handshake, Err(ref error) if matches!(error.error(), failure::Error::Protocol(failure::wire::Fault::ChannelNotReady {
                            channel: 1,
                            phase: failure::wire::ConnPhase::Start,
                            closing: false,
                        }))));
                    let failed = conn.observe().read(|view| view.is_failed()).await;
                    assert!(failed);
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
        let (sent, closed, redial) = broker
            .finish()
            .expect("illegal handshake broker")
            .expect("broker protocol");
        assert!(!sent, "client emitted Channel.FlowOk before readiness");
        assert!(closed, "client kept the invalid transport alive");
        assert_eq!(
            redial,
            Redial::Elapsed,
            "client redialed after a fatal phase violation"
        );
    })
    .expect("bind broker");
}

#[test]
fn rejects_server_locale() {
    handshake((0, 9), b"fr_FR", |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let handshake = conn.observe().epoch().await;
                    assert!(matches!(handshake, Err(ref error) if matches!(error.error(), failure::Error::UnsupportedLocale)));
                    let failed = conn.observe().read(|view| view.is_failed()).await;
                    assert!(failed);
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
        assert!(
            broker
                .finish()
                .expect("fatal handshake broker")
                .expect("broker protocol")
                == Redial::Elapsed,
            "connector redialed after an unsupported AMQP locale"
        );
    })
    .expect("bind broker");
}

#[test]
fn close_is_terminal() {
    close(true, |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let target = conn.observe().ready().await.expect("ready");

                    let allocation = Tracker::begin();
                    conn.control()
                        .close(target, 200, "done")
                        .await
                        .expect("graceful close");
                    let allocation = allocation.finish();
                    assert_eq!(
                        (allocation.allocations, allocation.allocated_bytes),
                        (0, 0)
                    );
                    let ready = conn.observe().read(|view| view.is_ready()).await;
                    assert!(!ready);
                    let handshake = conn.observe().epoch().await;
                    assert!(matches!(handshake, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
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
        assert!(
            broker
                .finish()
                .expect("terminal close broker")
                .expect("broker protocol")
                == Redial::Elapsed,
            "connector redialed after acknowledged local close"
        );
    })
    .expect("bind broker");
}

#[test]
fn close_loss_is_terminal() {
    close(false, |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                    let target = conn.observe().ready().await.expect("ready");

                    let close = conn.control().close(target, 200, "done").await;
                    assert!(matches!(close, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                    let ready = conn.observe().read(|view| view.is_ready()).await;
                    assert!(!ready);
                    let handshake = conn.observe().epoch().await;
                    assert!(matches!(handshake, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
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
        assert!(
            broker
                .finish()
                .expect("terminal close broker")
                .expect("broker protocol")
                == Redial::Elapsed,
            "connector redialed after interrupted local close"
        );
    })
    .expect("bind broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
