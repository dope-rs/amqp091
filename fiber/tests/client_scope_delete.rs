use std::time::Duration;

use dope_amqp091::message::field::Table;
use dope_amqp091::{config, failure};
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
fn prevents_replay() {
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

            Method::read(&mut first)
                .expect("first channel open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first channel open-ok");
            for (request, response) in [(20, 21), (40, 41)] {
                let operation = Method::read(&mut first).expect("external queue operation");
                operation
                    .check(1, 50, request)
                    .expect("external queue route");
                assert_eq!(queue_name(&operation).expect("queue name"), "external");
                let args = if response == 41 { &[0_u8; 4][..] } else { &[] };
                Method::write(&mut first, 1, 50, response, args).expect("external queue reply");
            }
            let consume = Method::read(&mut first).expect("name-only consumer");
            check_consume_target(&consume, "shared", "name-only").expect("consumer target");
            Method::write(&mut first, 1, 60, 21, b"\x09name-only").expect("name-only consume-ok");
            Method::read(&mut first)
                .expect("queue declare")
                .check(1, 50, 10)
                .expect("AMQP method route");
            first
                .write_all(&queue_declare_ok_frame(1, b"shared").expect("AMQP fixture"))
                .expect("queue declare-ok");
            Method::read(&mut first)
                .expect("consumer")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let mut consumed = Vec::new();
            put_short(&mut consumed, b"deleted-queue").expect("AMQP short string");
            first
                .write_all(&Method::try_encode(1, 60, 21, &consumed).expect("AMQP frame fixture"))
                .expect("consume-ok");

            Method::read(&mut first)
                .expect("second channel open")
                .check(2, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("second channel open-ok");
            Method::read(&mut first)
                .expect("cross-channel queue delete")
                .check(2, 50, 40)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(2, 50, 41, &0_u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("queue delete-ok");
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
            let purge = Method::read(&mut second).expect("deleted current queue purge");
            purge.check(1, 50, 30).expect("no deleted topology replay");
            assert_eq!(queue_name(&purge).expect("deleted queue name"), "shared");
            let mut close = Vec::from(404_u16.to_be_bytes());
            put_short(&mut close, b"missing shared").expect("reply text");
            close.extend_from_slice(&50_u16.to_be_bytes());
            close.extend_from_slice(&30_u16.to_be_bytes());
            Method::write(&mut second, 1, 20, 40, &close).expect("deleted queue rejection");
            Method::read(&mut second)
                .expect("channel close-ok")
                .check(1, 20, 41)
                .expect("close-ok route");
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
                            let first = conn
                                .channels()
                                .open_in(target, 1)
                                .await
                                .expect("first channel");
                            let args = Table::new();
                            first.queue().bind("external", "amq.direct", "rk", &args)
                                .await.expect("external queue binding");
                            first.queue().delete("external", false, false)
                                .await.expect("external queue delete");
                            let request = basic::Consume {
                                queue: "shared",
                                consumer_tag: "deleted-queue",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            };
                            let named = first.basic().consume(basic::Consume {
                                consumer_tag: "name-only",
                                ..request
                            }).await.expect("name-only consumer");
                            first
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
                            let consumer = first
                                .basic()
                                .consume(request)
                                .await
                                .expect("consumer");
                            let second = conn
                                .channels()
                                .open_in(target, 2)
                                .await
                                .expect("second channel");
                            second
                                .queue()
                                .delete("shared", false, false)
                                .await
                                .expect("cross-channel queue delete");

                            conn.observe().retired(target.epoch()).await.expect("first retirement");
                            conn.observe().ready().await.expect("complete replay");
                            let error = first.into_auto().queue().purge("").await
                                .expect_err("deleted current queue");
                            assert!(matches!(error.error(), failure::Error::Server {
                                reply_code: 404, reply_text, class_id: 50, method_id: 30,
                            } if reply_text.as_ref() == "missing shared"));
                            drop((named, consumer));
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("cross-channel topology broker");
        },
    )
    .expect("cross-channel topology broker");
}
