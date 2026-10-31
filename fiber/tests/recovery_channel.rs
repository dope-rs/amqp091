use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::recovery::broker::tagged;
pub mod common;

use std::io::{Read as _, Write as _};
use std::time::Duration;

use dope::core::platform::wake;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::ack::Disposition;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{basic, queue};
use dope_test::fibers::Yield;
use dope_test::harness::peer::{AcceptScript, Stream};

use crate::common::recovery::TUNE;
use crate::common::recovery::broker::{channel_close, open_named, queue_declare_ok};
use crate::common::recovery::client::{with, with_config};

fn assert_close_retired(mut stream: &Stream<'_>) {
    Method::read(&mut stream)
        .expect("Channel.CloseOk")
        .check(1, 20, 41)
        .expect("AMQP method route");
    let mut byte = [0_u8; 1];
    assert_eq!(
        stream.read(&mut byte).expect("read client retirement"),
        0,
        "client must retire the exact transport after draining Channel.CloseOk"
    );
}

#[test]
fn replays_consumer() {
    let (wait, ready) = wake::Ends::blocking().expect("consumer readiness").split();
    let (ack_rx, acked) = wake::Ends::blocking()
        .expect("acknowledgement observation")
        .split();
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
            open_named(&mut first, 1, "q", "tag").expect("AMQP fixture");
            wait.wait_timeout(Duration::from_secs(10))
                .expect("initial consumer readiness");

            first
                .write_all(
                    &channel_close(1, 404, "queue disappeared", 50, 10).expect("AMQP fixture"),
                )
                .expect("Channel.Close(404)");
            assert_close_retired(&first);

            let mut second = acceptor.accept().expect("replacement connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            open_named(&mut second, 1, "q", "tag").expect("AMQP fixture");
            second
                .write_all(&tagged(1, &[]).expect("AMQP delivery"))
                .expect("recovered delivery");
            Method::read(&mut second)
                .expect("Basic.Ack")
                .check(1, 60, 80)
                .expect("AMQP method route");
            acked.notify().expect("publish recovered delivery ACK");
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();

                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let target = conn.observe().ready().await.expect("initial ready");
                    let channel = conn
                        .channels()
                        .open_in(target, 1)
                        .await
                        .expect("channel");
                    let empty = Table::new();
                    channel
                        .queue()
                        .declare(queue::Declare {
                            queue: "q",
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            arguments: &empty,
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
                            arguments: &empty,
                        })
                        .await
                        .expect("consumer");
                    ready.notify().expect("publish initial consumer readiness");

                    while conn
                        .observe()
                        .read(|view| view.ready_cycle())
                        .await
                        != Some(2)
                    {
                        Yield::new().await;
                    }
                    conn.observe().read(|view| {
                        let cause = (view.last_exit()).expect("retained Channel.Close cause");
                        assert_eq!(cause.generation(), 1);
                        assert_eq!(cause.error().server_code(), Some(404));
                    }).await;

                    let recovered = consumer
                        .recv()
                        .await
                        .expect("recovered receive")
                        .expect("recovered delivery");
                    recovered
                        .settle(Disposition::Ack)
                        .await
                        .expect("generation-2 ACK");
                    common::fiber::wait(&ack_rx).await.expect("recovered delivery ACK");
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
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn replays_pending_queue() {
    let (wait, ready) = wake::Ends::blocking().expect("queue commit gate").split();
    AcceptScript::with(
        move |acceptor| {
            for epoch in 1..=3 {
                let mut stream = acceptor.accept().expect("connection");
                stream
                    .read_wait(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                stream
                    .write_wait(Some(Duration::from_secs(10)))
                    .expect("write timeout");
                Peer::new(&mut stream).open(TUNE).expect("handshake");
                Method::read(&mut stream)
                    .expect("channel open")
                    .check(1, 20, 10)
                    .expect("channel route");
                Method::write(&mut stream, 1, 20, 11, &[0, 0, 0, 0]).expect("channel open-ok");
                let request = Method::read(&mut stream).expect("queue declare");
                request.check(1, 50, 10).expect("queue route");
                assert_eq!(
                    crate::common::client::queue_name(&request).expect("queue name"),
                    ""
                );
                if epoch == 2 {
                    stream
                        .write_all(
                            &channel_close(1, 404, "queue disappeared", 50, 10)
                                .expect("channel close"),
                        )
                        .expect("pending refresh close");
                    assert_close_retired(&stream);
                    continue;
                }
                let name = if epoch == 1 { "old" } else { "new" };
                stream
                    .write_all(&queue_declare_ok(1, name, 0, 0).expect("queue reply"))
                    .expect("queue declare-ok");
                if epoch == 1 {
                    wait.wait_timeout(Duration::from_secs(10))
                        .expect("initial queue committed");
                } else {
                    Method::read(&mut stream)
                        .expect("connection close")
                        .check(0, 10, 50)
                        .expect("connection route");
                    Method::write(&mut stream, 0, 10, 51, &[]).expect("connection close-ok");
                }
            }
        },
        |broker| {
            with_config(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .topology(config::Topology::default().queues(1).identities(1)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("initial ready");
                        let channel = conn.channels().open_in(target, 1).await.expect("channel");
                        let args = Table::new();
                        channel.queue().declare(queue::Declare {
                            queue: "",
                            durable: false,
                            exclusive: true,
                            auto_delete: false,
                            arguments: &args,
                        }).await.expect("anonymous queue");
                        ready.notify().expect("initial queue committed");
                        conn.observe().retired(target.epoch()).await.expect("first retirement");
                        let target = conn.observe().ready().await.expect("recovered ready");
                        conn.observe().read(|view| {
                            assert_eq!(view.ready_cycle(), Some(3));
                            let cause = view.last_exit().expect("pending refresh close");
                            assert_eq!(cause.generation(), 2);
                            assert_eq!(cause.error().server_code(), Some(404));
                        }).await;
                        conn.control().close(target, 200, "done").await.expect("connection close");
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
            broker.finish().expect("queue recovery broker");
        },
    )
    .expect("queue recovery script");
}

fn assert_terminal_close(code: u16, text: &str) {
    let (wait, ready) = wake::Ends::blocking()
        .expect("terminal channel gate")
        .split();
    AcceptScript::with(
        move |acceptor| {
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
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("Channel.OpenOk");
            wait.wait_timeout(Duration::from_secs(10))
                .expect("terminal channel readiness");
            stream
                .write_all(&channel_close(1, code, text, 50, 10).expect("AMQP fixture"))
                .expect("terminal Channel.Close");
            assert_close_retired(&stream);
        },
        |broker| {
            with(
                broker.addr(),
                |rt| {
                    let conn = rt.connection();

                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("ready");
                            conn
                                .channels()
                                .open_in(target, 1)
                                .await
                                .expect("channel");
                            ready.notify().expect("publish terminal channel readiness");
                            conn
                                .observe()
                                .retired(target.epoch())
                                .await
                                .expect("terminal channel retirement");
                            let result = conn.observe().ready().await;
                            let error = result.expect_err("Channel.Close was not terminal");
                            assert_eq!(error.error().server_code(), Some(code));
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
fn preserves_fatal_close() {
    for (code, text) in [
        (403, "access refused"),
        (405, "resource locked"),
        (406, "declaration mismatch"),
        (599, "unrecognized channel failure"),
    ] {
        assert_terminal_close(code, text);
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
