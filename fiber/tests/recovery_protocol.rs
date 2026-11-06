use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope::manifold::timing;
use dope_amqp091::config;
use dope_amqp091::failure::Runtime;

use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};
use crate::common::recovery::fault::raw_peer;
pub mod common;
use std::time::Duration;

use common::recovery::fault::Violation;
use dope_amqp091::protocol::wire::frame;
use dope_test::harness::peer::AcceptScript;

use crate::common::fiber::{Pinned, poll_once};
use crate::common::recovery::client::{POLICY, with};

fn check_connection_close(trigger: Vec<u8>, max: u32, code: u16) {
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
                .open(Tune::new(16, max, 0))
                .expect("AMQP broker handshake");
            Method::read(&mut stream)
                .expect("ready channel request")
                .check(1, 20, 10)
                .expect("Channel.Open");
            stream.write_all(&trigger).expect("protocol violation");
            let close = Method::read(&mut stream).expect("Connection.Close");
            close.check(0, 10, 50).expect("AMQP method route");
            let actual = close.1[4..6]
                .try_into()
                .expect("reply code occupies two octets");
            assert_eq!(u16::from_be_bytes(actual), code);
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
                            let target = conn.observe().ready().await.expect("epoch");
                            assert!(conn.channels().open_in(target, 1).await.is_err());
                            conn
                                .observe()
                                .retired(target.epoch())
                                .await
                                .expect("protocol close retirement");
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
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn reports_oversized_frame() {
    let mut header = vec![1, 0, 0];
    header.extend_from_slice(&5000_u32.to_be_bytes());
    check_connection_close(header, 4096, 501);
}

#[test]
fn reports_unknown_command() {
    check_connection_close(
        Method::try_encode(0, 10, 999, &[]).expect("AMQP frame fixture"),
        131_072,
        503,
    );
}

#[test]
fn reports_truncated_method() {
    check_connection_close(
        Method::try_encode(0, 10, 60, &[2, b'x']).expect("AMQP frame fixture"),
        131_072,
        502,
    );
}

#[test]
fn reports_unknown_channel() {
    check_connection_close(
        Method::try_encode(7, 20, 20, &[1]).expect("AMQP frame fixture"),
        131_072,
        504,
    );
}

#[test]
fn stops_replay_when_protocol_close_starts() {
    AcceptScript::with(
        |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(5)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(5)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(Tune::new(16, 131_072, 1))
                .expect("first handshake");
            for channel in 1..=2 {
                Method::read(&mut first)
                    .expect("channel open")
                    .check(channel, 20, 10)
                    .expect("channel open route");
                Method::write(&mut first, channel, 20, 11, &[0; 4]).expect("channel open-ok");
            }
            Method::read(&mut first)
                .expect("initial topology observed")
                .check(1, 50, 30)
                .expect("queue purge route");
            drop(first);

            let mut second = acceptor.accept().expect("replacement connection");
            second
                .read_wait(Some(Duration::from_secs(5)))
                .expect("replacement read timeout");
            second
                .write_wait(Some(Duration::from_secs(5)))
                .expect("replacement write timeout");
            Peer::new(&mut second)
                .open(Tune::new(16, 131_072, 1))
                .expect("replacement handshake");
            let open = Method::read(&mut second).expect("replayed channel open");
            let channel = open.channel();
            assert!(matches!(channel, 1 | 2));
            open.check(channel, 20, 10).expect("first replay route");
            let mut response =
                Method::try_encode(channel, 20, 11, &[0; 4]).expect("replayed channel reply");
            response
                .extend_from_slice(&Method::try_encode(0, 10, 999, &[]).expect("unknown command"));
            second
                .write_all(&response)
                .expect("replay protocol failure");
            let mut close = Method::read(&mut second).expect("protocol close or remaining open");
            if close.channel() != 0 {
                close
                    .check(3 - channel, 20, 10)
                    .expect("remaining replay route");
                close = Method::read(&mut second).expect("protocol close");
            }
            close.check(0, 10, 50).expect("protocol close route");
            assert_eq!(
                second.read(&mut [0]).expect("close retirement"),
                0,
                "replay emitted a frame after connection.close"
            );
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .tune(config::Tune::default().heartbeat(1));
            Endpoint::new(broker.addr(), config)
                .shutdown(broker.completion())
                .try_check::<timing::Throughput, _>(
                    POLICY,
                    |rt| {
                        let conn = rt.connection();
                        rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                            let first = conn.channels().open(1).await.expect("first channel");
                            conn.channels().open(2).await.expect("second channel");
                            assert!(first.queue().purge("retained").await.is_err());
                            let error = conn.observe().ready().await.expect_err("failed replay");
                            std::assert_matches!(
                                error.error(),
                                dope_amqp091::failure::Error::UnknownMethod {
                                    class_id: 10,
                                    method_id: 999,
                                }
                            );
                            conn.observe().read(|view| {
                                assert_eq!(view.generation(), 2);
                                assert_eq!(view.ready_cycle(), None);
                            }).await;
                        }))
                        .expect("broker shutdown")
                        .expect("replay failure observation");
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
fn rejects_invalid_end() {
    raw_peer(Violation::InvalidEnd, move |broker| {
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
                    let target = conn.observe().ready().await.expect("epoch");
                    assert!(conn.channels().open_in(target, 1).await.is_err());
                    conn
                        .observe()
                        .retired(target.epoch())
                        .await
                        .expect("raw transport retirement");
                    let error = conn.observe().epoch().await.expect_err("fatal frame violation");
                    std::assert_matches!(error.error(),
                        dope_amqp091::failure::Error::Frame(frame::Fault::Violation(
                            frame::Violation::InvalidEnd(0)
                        ))
                    );
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
        broker
            .finish()
            .expect("scripted broker thread")
            .expect("broker protocol");
    })
    .expect("bind raw peer");
}

#[test]
fn stops_without_redial() {
    raw_peer(Violation::UndefinedType(99), move |broker| {
        Endpoint::new(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
        )
        .shutdown(broker.completion())
        .try_check::<timing::Throughput, _>(
            POLICY,
            |rt| {
                let conn = rt.connection();
                let result = rt
                    .until_shutdown(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("epoch");
                        let mut retire = Pinned::new(conn.observe().retired(target.epoch()));
                        assert!(poll_once(retire.as_pin()).await.is_pending());
                        assert!(conn.channels().open_in(target, 1).await.is_err());
                        retire.await.expect("fatal transport retirement");
                        conn.observe().epoch().await
                    }))
                    .expect("broker completion")
                    .expect("AMQP fiber must complete");
                let error = result.expect_err("frame violation allowed service recovery");
                let (cause,) = match error {
                    Runtime::Terminal(cause) => Some((cause,)),
                    _ => None,
                }
                .expect("frame violation did not preserve its transport cause");
                assert_eq!(cause.generation(), 1);
                std::assert_matches!(
                    cause.error(),
                    dope_amqp091::failure::Error::Frame(frame::Fault::Violation(
                        frame::Violation::UndefinedType(99)
                    ))
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
        broker
            .finish()
            .expect("scripted broker thread")
            .expect("broker protocol");
    })
    .expect("bind raw peer");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
