use std::convert::Infallible;
use std::io::Write as _;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::message::field::Table;
use dope_amqp091::{config, failure};
use dope_amqp091_fiber::channel::queue;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::client::queue_name;

pub mod common;

use std::sync::mpsc;

use dope_test::fibers::Yield;
use dope_test::harness::peer::AcceptScript;

use crate::common::client::*;

#[test]
fn restores_current_queue() {
    for mismatch in [false, true] {
        let (done, finish) = mpsc::channel();
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
                for channel in [1, 2] {
                    Method::read(&mut first)
                        .expect("channel open")
                        .check(channel, 20, 10).expect("AMQP method route");
                    first
                        .write_all(
                            &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                                .expect("AMQP frame fixture"),
                        )
                        .expect("channel open-ok");
                }

                let declared = Method::read(&mut first).expect("server-named queue declare");
                declared.check(1, 50, 10).expect("AMQP method route");
                assert!(
                    queue_name(&declared).expect("queue name").is_empty(),
                    "server-named request"
                );
                first
                    .write_all(&queue_declare_ok_frame(1, b"first.generated").expect("AMQP fixture"))
                    .expect("server-named queue declare-ok");

                let selected = Method::read(&mut first).expect("second channel queue declare");
                selected.check(2, 50, 10).expect("AMQP method route");
                assert_eq!(
                    queue_name(&selected).expect("queue name").as_bytes(),
                    b"first.generated",
                    "second channel current queue"
                );
                first
                    .write_all(&queue_declare_ok_frame(2, b"first.generated").expect("AMQP fixture"))
                    .expect("second channel queue declare-ok");
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
                let mut opened = [false; 3];
                for _ in 0..2 {
                    let open = Method::read(&mut second).expect("recovered channel open");
                    assert_eq!(open.class(), 20, "recovered channel open class");
                    assert_eq!(open.method(), 10, "recovered channel open method");
                    let channel = usize::from(open.channel());
                    std::assert_matches!(channel, 1 | 2, "recovered channel id");
                    assert!(!opened[channel], "duplicate recovered channel open");
                    opened[channel] = true;
                    second
                        .write_all(
                            &Method::try_encode(open.channel(), 20, 11, &[0, 0, 0, 0])
                                .expect("AMQP frame fixture"),
                        )
                        .expect("recovered channel open-ok");
                }

                let replay = Method::read(&mut second).expect("global queue replay");
                assert_eq!(replay.class(), 50, "global queue replay class");
                assert_eq!(replay.method(), 10, "global queue replay method");
                assert!(
                    queue_name(&replay).expect("queue name").is_empty(),
                    "global server-named replay"
                );
                let source = replay.channel();
                second
                    .write_all(
                        &queue_declare_ok_frame(source, b"second.generated").expect("AMQP fixture"),
                    )
                    .expect("global queue replay-ok");

                let current = Method::read(&mut second).expect("channel current queue restore");
                assert_eq!(current.class(), 50, "current queue restore class");
                assert_eq!(current.method(), 10, "current queue restore method");
                assert_ne!(
                    current.channel(),
                    source,
                    "global replay channel already has a current queue"
                );
                assert_eq!(
                    queue_name(&current).expect("queue name").as_bytes(),
                    b"second.generated",
                    "restored current queue identity"
                );
                second
                    .write_all(
                        &queue_declare_ok_frame(
                            current.channel(),
                            if mismatch {
                                b"different.generated"
                            } else {
                                b"second.generated"
                            },
                        )
                        .expect("AMQP fixture"),
                    )
                    .expect("channel current queue restore-ok");
                if mismatch {
                    let close = Method::read(&mut second).expect("queue name mismatch close");
                    close.check(0, 10, 50).expect("connection close");
                    assert_eq!(
                        close.arguments().get(..2),
                        Some(504_u16.to_be_bytes().as_slice())
                    );
                    second
                        .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP fixture"))
                        .expect("connection close-ok");
                }
                done.send(()).expect("publish current queue restoration");

                if mismatch {
                    return;
                }
                for channel in [1, 2] {
                    let purge = Method::read(&mut second).expect("current queue purge");
                    purge.check(channel, 50, 30).expect("AMQP method route");
                    assert!(
                        queue_name(&purge).expect("queue name").is_empty(),
                        "current queue reference"
                    );
                    second
                        .write_all(
                            &Method::try_encode(channel, 50, 31, &0_u32.to_be_bytes())
                                .expect("AMQP frame fixture"),
                        )
                        .expect("current queue purge-ok");
                }
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest").expect("config"),
                    |rt| {
                        let conn = rt.connection();
                        assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("first ready");
                            let epoch = conn.observe().read(|view| view.generation()).await;
                            let first = conn
                                .channels()
                                .open_in(target, 1)
                                .await
                                .expect("first channel");
                            let second = conn
                                .channels()
                                .open_in(target, 2)
                                .await
                                .expect("second channel");
                            let args = Table::new();
                            let declared = first
                                .queue()
                                .declare(queue::Declare {
                                    queue: "",
                                    durable: false,
                                    exclusive: true,
                                    auto_delete: true,
                                    arguments: &args,
                                })
                                .await
                                .expect("server-named queue declare");
                            second
                                .queue()
                                .declare(queue::Declare {
                                    queue: declared.queue.as_ref(),
                                    durable: false,
                                    exclusive: true,
                                    auto_delete: true,
                                    arguments: &args,
                                })
                                .await
                                .expect("second channel queue declare");

                            while conn.observe().read(|view| view.generation()).await == epoch {
                                Yield::new().await;
                            }
                            if mismatch {
                                let error = conn
                                    .observe()
                                    .ready()
                                    .await
                                    .expect_err("mismatched current queue");
                                std::assert_matches!(
                                    error.error(),
                                    failure::Error::QueueName { expected, actual }
                                        if expected.as_ref() == "second.generated"
                                            && actual.as_ref() == "different.generated"
                                );
                            }
                            loop {
                                match finish.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before current queue restoration"), }
                            }
                            if mismatch {
                                return;
                            }
                            let first = first.into_auto();
                            let second = second.into_auto();
                            first
                                .queue()
                                .purge("")
                                .await
                                .expect("first current queue purge");
                            second
                                .queue()
                                .purge("")
                                .await
                                .expect("second current queue purge");
                        })).is_ok(), "AMQP fiber must complete");
                    }, recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                broker.finish().expect("channel current queue broker");
            },
        )
        .expect("channel current queue broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
