use crate::common::amqp::wire::Method;
use crate::common::amqp::{Greeting, Peer, Tune};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use std::time::Duration;

use dope_amqp091::failure;
use dope_amqp091::message::field::{Table, Value};
use dope_test::harness::peer::TcpScript;

use crate::common::recovery::client::with;

#[test]
fn rejects_large_method() {
    TcpScript::with(
        move |stream| {
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .expect("write timeout");
            Peer::new(stream)
                .open(Tune::new(16, 4096, 0))
                .expect("AMQP broker handshake");
            Method::read(stream)
                .expect("Channel.Open")
                .check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("Channel.OpenOk");

            Method::read(stream)
                .expect("method after local rejection")
                .check(1, 50, 30)
                .expect("oversized method must not precede Queue.Purge");
            Method::write(stream, 1, 50, 31, &[0; 4]).expect("Queue.PurgeOk");
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel");
                    let value = Value::string(vec![b'x'; 5_000]).expect("5000-byte AMQP field value");
                    let mut args = Table::new();
                    args.insert("oversized", value).expect("first table field");
                    let error = channel.queue().bind("queue", "exchange", "#", &args).await.expect_err("oversized method passed frame-max");
                    std::assert_matches!(
                        error.error(),
                        failure::Error::FrameTooLarge { max: 4096, .. }
                    );
                    channel.queue().purge("queue").await.expect("transport survives local rejection");
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn rejects_small_frame_max() {
    TcpScript::with(
        move |stream| {
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .expect("write timeout");

            Peer::new(stream)
                .preface()
                .expect("AMQP preface")
                .start(Greeting::PLAIN)
                .expect("Connection.Start")
                .recv()
                .expect("Connection.StartOk")
                .tune(Tune::new(16, 1024, 0))
                .expect("invalid Connection.Tune");
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();
                    let error = rt
                        .block_on(conn.observe().epoch())
                        .expect("sub-minimum server frame-max must fail the handshake")
                        .expect_err("sub-minimum server frame-max must fail the handshake");
                    std::assert_matches!(error.error(), failure::Error::Protocol(_));
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}
