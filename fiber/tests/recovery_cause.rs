use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
pub mod common;
use std::time::Duration;

use dope_amqp091::failure;
use dope_test::harness::peer::{AcceptScript, TcpScript};

use crate::common::recovery::TUNE;
use crate::common::recovery::client::with;

#[test]
fn retains_close_when_egress_is_full() {
    TcpScript::with(
        |stream| {
            stream
                .read_wait(Some(Duration::from_secs(10)))
                .expect("read deadline");
            Peer::new(stream)
                .preface()
                .expect("preface")
                .start(common::amqp::Greeting::PLAIN)
                .expect("start")
                .recv()
                .expect("start-ok");
            let mut batch = Method::try_encode(0, 10, 20, &[0; 4]).expect("empty challenge");
            batch.extend_from_within(..);
            batch.extend_from_slice(
                &Method::try_encode(0, 10, 50, &[1, 147, 0, 0, 10, 0, 40]).expect("close"),
            );
            stream
                .write_all(&batch)
                .expect("two challenges followed by close");
            assert_eq!(
                stream.read(&mut [0]).expect("retired transport"),
                0,
                "the full egress queue must be discarded before transmission"
            );
        },
        |broker| {
            let config = dope_amqp091::config::Config::new("guest", "guest")
                .expect("config")
                .capacity(
                    dope_amqp091::config::Capacity::default()
                        .egress(dope_amqp091::config::Egress::new(2, 4096)),
                );
            common::recovery::client::with_config(
                broker.addr(),
                config,
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                    let error = conn.observe().ready().await.expect_err("broker refused opening");
                    assert!(matches!(error.error(), failure::Error::Server {
                        reply_code: 403, class_id: 10, method_id: 40, ..
                    }), "lost broker cause: {error:?}");
                })).expect("fiber completion");
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
    .expect("full egress broker");
}

#[test]
fn retains_disconnect_cause() {
    const CODE: u16 = 320;
    const TEXT: &str = "scenario rollover";

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
                .expect("first ready channel request")
                .check(1, 20, 10)
                .expect("Channel.Open");
            let mut args = Vec::with_capacity(TEXT.len() + 7);
            args.extend_from_slice(&CODE.to_be_bytes());
            args.push(TEXT.len().try_into().expect("short close text"));
            args.extend_from_slice(TEXT.as_bytes());
            args.extend_from_slice(&0_u16.to_be_bytes());
            args.extend_from_slice(&0_u16.to_be_bytes());
            first
                .write_all(&Method::try_encode(0, 10, 50, &args).expect("AMQP frame fixture"))
                .expect("Connection.Close");
            Method::read(&mut first)
                .expect("Connection.CloseOk")
                .check(0, 10, 51).expect("AMQP method route");
            drop(first);

            let mut second = acceptor.accept().expect("second connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            while second.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        move |broker| {
            with(
                broker.addr(),
                move |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("first ready");
                    assert!(conn.channels().open_in(ready, 1).await.is_err());
                    conn.observe().ready().await.expect("reconnected ready");
                    conn.observe().read(|view| {
                        assert_eq!(view.generation(), 2);
                        let cause = (view.last_exit()).expect("generation 1 disconnect cause");
                        assert_eq!(cause.generation(), 1);
                        let (reply_code, reply_text, class_id, method_id,) = match cause.error() { failure::Error::Server {
                            reply_code,
                            reply_text,
                            class_id,
                            method_id,
                        } => Some((reply_code, reply_text, class_id, method_id,)), _ => None, }.expect("server close evidence was lost");
                        assert_eq!(*reply_code, CODE);
                        assert_eq!(reply_text.as_ref(), TEXT);
                        assert_eq!((*class_id, *method_id), (0, 0));
                    }).await;
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
