use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_amqp091::failure::Runtime;

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};
use crate::common::recovery::fault::raw_peer;
pub mod common;
use std::time::Duration;

use common::recovery::fault::Violation;
use dope::core::platform::wake;
use dope_amqp091::protocol::wire::frame;
use dope_test::harness::peer::AcceptScript;

use crate::common::fiber::{Pinned, poll_once, wait};
use crate::common::recovery::client::with;

fn check_connection_close(trigger: Vec<u8>, max: u32, code: u16) {
    let (gate, ready) = wake::Ends::blocking()
        .expect("protocol violation gate")
        .split();
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
            gate.wait_timeout(Duration::from_secs(5))
                .expect("handshake observation");
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
                            let target = conn.observe().epoch().await.expect("epoch");
                            ready.notify().expect("protocol violation gate");
                            conn
                                .observe()
                                .retired(target)
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
fn rejects_invalid_end() {
    let (gate, ready) = wake::Ends::blocking().expect("ready pipe").split();
    raw_peer(Violation::InvalidEnd, gate, None, move |broker, closed| {
        with(
            broker.addr(),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().epoch().await.expect("epoch");
                        ready.notify().expect("raw violation gate");
                        conn
                            .observe()
                            .retired(target)
                            .await
                            .expect("raw transport retirement");
                        wait(&closed).await.expect("raw-close observation");
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
        broker
            .finish()
            .expect("scripted broker thread")
            .expect("broker protocol");
    })
    .expect("bind raw peer");
}

#[test]
fn stops_without_redial() {
    let (gate, ready) = wake::Ends::blocking().expect("ready pipe").split();
    let (finish, done) = wake::Ends::blocking().expect("completion pipe").split();
    raw_peer(
        Violation::UndefinedType(99),
        gate,
        Some(finish),
        move |broker, closed| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    let result = (rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().epoch().await.expect("epoch");
                        let mut retire = Pinned::new(conn.observe().retired(target));
                        assert!(poll_once(retire.as_pin()).await.is_pending());
                        ready.notify().expect("peer violation gate");
                        retire.await.expect("fatal transport retirement");
                        let result = conn.observe().epoch().await;
                        done.notify().expect("peer recovery gate");
                        wait(&closed).await.expect("raw-close observation");
                        result
                    })))
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
        },
    )
    .expect("bind raw peer");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
