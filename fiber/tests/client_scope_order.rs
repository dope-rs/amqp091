use std::time::Duration;

use dope_amqp091::config;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{basic, queue};

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short};

use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_amqp091::message::ack;
use dope_test::harness::peer::AcceptScript;

use crate::common::client::*;

#[test]
fn replays_dependency_first() {
    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("first connection");
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
                    .check(channel, 20, 10)
                    .expect("AMQP method route");
                first
                    .write_all(
                        &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                            .expect("AMQP frame fixture"),
                    )
                    .expect("channel open-ok");
            }
            Method::read(&mut first)
                .expect("queue declare")
                .check(2, 50, 10)
                .expect("AMQP method route");
            first
                .write_all(&queue_declare_ok_frame(2, b"shared").expect("AMQP fixture"))
                .expect("queue declare-ok");
            Method::read(&mut first)
                .expect("consumer")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let mut consumed = Vec::new();
            put_short(&mut consumed, b"cross-channel").expect("AMQP short string");
            first
                .write_all(&Method::try_encode(1, 60, 21, &consumed).expect("AMQP frame fixture"))
                .expect("consume-ok");
            drop(first);

            let mut second = broker.accept().expect("recovered connection");
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
            assert!(opened[1] && opened[2], "both channels must recover");
            let declare = Method::read(&mut second).expect("recovered queue declare");
            assert_eq!(declare.class(), 50, "recovered queue declare class");
            assert_eq!(declare.method(), 10, "recovered queue declare method");
            assert!(
                opened[usize::from(declare.channel())],
                "resource replay channel"
            );
            second
                .write_all(
                    &queue_declare_ok_frame(declare.channel(), b"shared").expect("AMQP fixture"),
                )
                .expect("recovered queue declare-ok");
            Method::read(&mut second)
                .expect("recovered consumer")
                .check(1, 60, 20)
                .expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 60, 21, &consumed).expect("AMQP frame fixture"))
                .expect("recovered consume-ok");
            Method::read(&mut second)
                .expect("connection close")
                .check(0, 10, 50)
                .expect("close after complete replay");
            Method::write(&mut second, 0, 10, 51, &[]).expect("connection.close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("first ready");
                            let source = conn
                                .channels()
                                .open_in(target, 1)
                                .await
                                .expect("consumer channel");
                            let resource = conn
                                .channels()
                                .open_in(target, 2)
                                .await
                                .expect("resource channel");
                            let args = Table::new();
                            resource
                                .queue()
                                .declare(queue::Declare {
                                    queue: "shared",
                                    durable: false,
                                    exclusive: false,
                                    auto_delete: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("queue declare");
                            let sub = source
                                .basic()
                                .consume(basic::Consume {
                                    queue: "shared",
                                    consumer_tag: "cross-channel",
                                    no_local: false,
                                    ack: ack::Manual,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("consumer");

                            conn.observe().retired(target.epoch()).await.expect("first retirement");
                            let ready = conn.observe().ready().await.expect("complete replay");
                            conn.control().close(ready, 200, "done").await.expect("close");
                            drop(sub);
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("cross-channel dependency broker");
        },
    )
    .expect("cross-channel dependency broker");
}
