use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
pub mod common;

use std::time::Duration;

use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091::{config, failure};
use dope_amqp091_fiber::channel::{basic, queue};
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::harness::peer::AcceptScript;

use crate::common::basic::returned;
use crate::common::fiber::{Pinned, poll_once};
use crate::common::recovery::TUNE;
use crate::common::recovery::broker::{Delivery, delivery_frames, open_named};
use crate::common::recovery::client::with_config;

#[test]
fn reuses_retired_slots() {
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
            open_named(&first, 1, "q", "tag").expect("AMQP fixture");
            Method::read(&mut first)
                .expect("Queue.Purge")
                .check(1, 50, 30)
                .expect("AMQP method route");
            drop(first);

            let mut second = broker.accept().expect("second connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            open_named(&second, 1, "q", "tag").expect("AMQP fixture");
            let mut frames = delivery_frames(Delivery {
                channel: 1,
                consumer_tag: "tag",
                delivery_tag: 1,
                redelivered: true,
                exchange: "",
                routing_key: "fresh-1",
                body: b"generation-2-delivery-1",
            })
            .expect("AMQP fixture");
            frames.extend_from_slice(
                &delivery_frames(Delivery {
                    channel: 1,
                    consumer_tag: "tag",
                    delivery_tag: 2,
                    redelivered: true,
                    exchange: "",
                    routing_key: "fresh-2",
                    body: b"generation-2-delivery-2",
                })
                .expect("AMQP fixture"),
            );
            frames.extend_from_slice(
                &returned::content(1, "", "fresh-1", &[b"generation-2-return-1"], None)
                    .expect("AMQP fixture"),
            );
            frames.extend_from_slice(
                &returned::content(1, "", "fresh-2", &[b"generation-2-return-2"], None)
                    .expect("AMQP fixture"),
            );
            second
                .write_all(&frames)
                .expect("generation-2 congested content");

            Method::read(&mut second)
                .expect("Queue.Purge")
                .check(1, 50, 30)
                .expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 50, 31, &0_u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("generation-2 PurgeOk");
        },
        |broker| {
            let addr = broker.addr();

            let chan = config::Channel::default()
                .methods(1)
                .consumers(1)
                .deliveries(1)
                .returns(1);
            let cfg = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(config::Capacity::default().channels(1).channel(chan));
            with_config(
                addr,
                cfg,
                |rt| {
                    let conn = rt.connection();
                    let timer = rt.timer();
                    let run = dope_fiber::fiber!('_ => async move {
                        let target = conn
                            .observe()
                            .ready()
                            .await
                            .expect("first ready");
                        let chan = conn
                            .channels()
                            .open_in(target, 1)
                            .await
                            .expect("channel");
                        let empty = Table::new();
                        chan
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
                        let mut sub = chan
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
                        let chan = chan.into_auto();

                        let mut recv = Pinned::new(sub.recv());
                        assert!(poll_once(recv.as_pin()).await.is_pending());
                        let mut reply = Pinned::new(chan.next_return());
                        assert!(poll_once(reply.as_pin()).await.is_pending());
                        let mut stale = Pinned::new(chan.queue().purge("generation-1"));
                        assert!(poll_once(stale.as_pin()).await.is_pending());
                        let fault = stale.await.expect_err("retired method crossed generations");
                        let cause = match fault {
                            failure::Runtime::Terminal(cause) => Some(cause),
                            failure::Runtime::Local(_) => None,
                        }.expect("transport retirement became a local failure");
                        assert_eq!(cause.generation(), 1);

                        while conn
                            .observe()
                            .read(|view| view.ready_cycle())
                            .await
                            != Some(2)
                        {
                            timer
                                .sleep(Duration::from_millis(1))
                                .expect("recovery wait")
                                .await;
                        }

                        let first = recv
                            .await
                            .expect("generation-2 receive")
                            .expect("generation-2 delivery");
                        assert!(first.is_repeat());
                        assert_eq!(first.routing_key(), "fresh-1");
                        assert!(first.body() == b"generation-2-delivery-1"[..]);
                        first.release();

                        let second = sub
                            .recv()
                            .await
                            .expect("second generation-2 receive")
                            .expect("second generation-2 delivery");
                        assert!(second.is_repeat());
                        assert_eq!(second.routing_key(), "fresh-2");
                        assert!(second.body() == b"generation-2-delivery-2"[..]);
                        second.release();

                        let first = reply
                            .await
                            .expect("generation-2 return")
                            .expect("returned message");
                        assert_eq!(first.reply_code(), 312);
                        assert_eq!(first.reply_text(), "NO_ROUTE");
                        assert_eq!(first.routing_key(), "fresh-1");
                        assert!(first.body() == b"generation-2-return-1"[..]);
                        first.release();

                        let second = chan
                            .next_return()
                            .await
                            .expect("second generation-2 return")
                            .expect("second returned message");
                        assert_eq!(second.reply_code(), 312);
                        assert_eq!(second.reply_text(), "NO_ROUTE");
                        assert_eq!(second.routing_key(), "fresh-2");
                        assert!(second.body() == b"generation-2-return-2"[..]);
                        second.release();

                        chan
                            .queue()
                            .purge("generation-2")
                            .await
                            .expect("generation-2 method slot reuse");
                    });
                    assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
