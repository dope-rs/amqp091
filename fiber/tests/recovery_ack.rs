use crate::common::amqp::wire::Method;
use crate::common::recovery::broker::{ack_tag, open_tagged, tagged};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;
use std::task::Poll;

use dope_amqp091::adapter::consumer;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::ack::Disposition;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{basic, queue};
use dope_test::harness::peer::AcceptScript;

use crate::common::fiber::{Pinned, poll_once};
use crate::common::recovery::client::{with, with_config};

#[test]
fn rejects_stale_epoch() {
    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("first connection");
            open_tagged(&first).expect("AMQP broker");
            first.write_all(&tagged(1, &[]).expect("AMQP delivery")).expect("first delivery");
            Method::read(&mut first).expect("first delivery observed")
                .check(1, 50, 30).expect("queue.purge");
            drop(first);

            let mut second = broker.accept().expect("second connection");
            open_tagged(&second).expect("AMQP broker");
            second.write_all(&tagged(1, &[]).expect("AMQP delivery")).expect("second delivery");

            Method::read(&mut second).expect("stale settlement fence")
                .check(1, 50, 30).expect("stale ACK must not precede queue.purge");
            Method::write(&mut second, 1, 50, 31, &[0; 4]).expect("queue.purge-ok");

            let ack = Method::read(&mut second).expect("Basic.Ack");
            ack.check(1, 60, 80).expect("AMQP method route");
            assert_eq!(ack_tag(&ack).expect("acknowledgement tag"), 1);

            second.write_all(&tagged(2, &[]).expect("AMQP delivery")).expect("requeue delivery");
            let nack = Method::read(&mut second).expect("Basic.Nack");
            nack.check(1, 60, 120).expect("AMQP method route");
            assert_eq!(ack_tag(&nack).expect("acknowledgement tag"), 2);
            assert_eq!(nack.1[12], 2, "individual NACK must set only requeue");

            second.write_all(&tagged(3, &[]).expect("AMQP delivery")).expect("reject delivery");
            let reject = Method::read(&mut second).expect("Basic.Reject");
            reject.check(1, 60, 90).expect("AMQP method route");
            assert_eq!(ack_tag(&reject).expect("acknowledgement tag"), 3);
            assert_eq!(reject.1[12], 1, "REJECT must preserve requeue");
            Method::read(&mut second).expect("close after settlements")
                .check(0, 10, 50).expect("connection.close");
            Method::write(&mut second, 0, 10, 51, &[]).expect("connection.close-ok");
        },
        move |broker| {
            with(
                broker.addr(),
                move |runtime| {
                    let conn = runtime.connection();
                    assert!(runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let target = conn
                        .observe()
                        .ready()
                        .await
                        .expect("first ready");
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
                    let stale = consumer
                        .recv()
                        .await
                        .expect("consumer active")
                        .expect("first delivery");
                    let stale_epoch = stale.epoch();
                    channel.queue().purge("q").await.expect_err("first transport closes");
                    conn.observe().retired(target.epoch()).await.expect("first retirement");
                    let ready = conn.observe().ready().await.expect("recovered ready");
                    let channel = channel.into_auto();
                    let current = consumer
                        .recv()
                        .await
                        .expect("consumer active")
                        .expect("second delivery");
                    assert!(current.epoch() != stale_epoch);
                    let error = stale.settle(Disposition::Ack).await.expect_err("stale target was rebound");
                    std::assert_matches!(error, consumer::Error::SourceUnavailable(_));
                    channel.in_ready(ready).queue().purge("q").await.expect("stale settlement fence");
                    current
                        .settle(Disposition::Ack)
                        .await
                        .expect("current settlement");
                    let requeue = consumer
                        .recv()
                        .await
                        .expect("consumer active")
                        .expect("requeue delivery");
                    requeue
                        .settle(Disposition::Nack { requeue: true })
                        .await
                        .expect("requeue settlement");
                    let reject = consumer
                        .recv()
                        .await
                        .expect("consumer active")
                        .expect("reject delivery");
                    reject
                        .settle(Disposition::Reject { requeue: true })
                        .await
                        .expect("reject settlement");
                    conn.control().close(ready, 200, "done").await.expect("close after settlements");
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
fn waits_for_egress_credit() {
    AcceptScript::with(
        move |broker| {
            let mut stream = broker.accept().expect("connection");
            open_tagged(&stream).expect("AMQP broker");
            stream
                .write_all(&tagged(1, &[]).expect("AMQP delivery"))
                .expect("first delivery");
            stream
                .write_all(&tagged(2, &[]).expect("AMQP delivery"))
                .expect("second delivery");
            stream
                .write_all(&tagged(3, &[]).expect("AMQP delivery"))
                .expect("third delivery");

            for expected in [1_u64, 3] {
                let ack = Method::read(&mut stream).expect("Basic.Ack");
                ack.check(1, 60, 80).expect("AMQP method route");
                assert_eq!(ack_tag(&ack).expect("acknowledgement tag"), expected);
            }
            Method::read(&mut stream)
                .expect("Connection.Close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("Connection.CloseOk");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().egress(config::Egress::new(1, 1024)));
            with_config(
                broker.addr(),
                config,
                |runtime| {
                    let conn = runtime.connection();
                    assert!(
                    runtime
                        .block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
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
                            let first = consumer
                                .recv()
                                .await
                                .expect("consumer active")
                                .expect("first delivery");
                            let second = consumer
                                .recv()
                                .await
                                .expect("consumer active")
                                .expect("second delivery");
                            let third = consumer
                                .recv()
                                .await
                                .expect("consumer active")
                                .expect("third delivery");

                            let mut first = Pinned::new(first.settle(Disposition::Ack));
                            let first_admission = poll_once(first.as_pin()).await;
                            std::assert_matches!(first_admission, Poll::Ready(Ok(())));

                            let mut second = Pinned::new(second.settle(Disposition::Ack));
                            assert!(poll_once(second.as_pin()).await.is_pending());
                            drop(second);

                            third
                                .settle(Disposition::Ack)
                                .await
                                .expect("replacement settlement resumes after exact egress credit");
                            conn
                                .control()
                                .close(ready, 200, "OK")
                                .await
                                .expect("close");
                        }))
                        .is_ok(),
                    "AMQP fiber must complete"
                );
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

#[test]
fn releases_body_first() {
    AcceptScript::with(
        move |broker| {
            let mut stream = broker.accept().expect("connection");
            open_tagged(&stream).expect("AMQP broker");
            stream
                .write_all(&tagged(1, b"a").expect("AMQP delivery"))
                .expect("first delivery");
            stream
                .write_all(&tagged(2, b"b").expect("AMQP delivery"))
                .expect("second delivery");

            for expected in [1_u64, 2] {
                let ack = Method::read(&mut stream).expect("Basic.Ack");
                ack.check(1, 60, 80).expect("AMQP method route");
                assert_eq!(ack_tag(&ack).expect("acknowledgement tag"), expected);
            }
            Method::read(&mut stream)
                .expect("Connection.Close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("Connection.CloseOk");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().body(config::Body::default().fragments(1)));
            with_config(
                broker.addr(),
                config,
                |runtime| {
                    let conn = runtime.connection();
                    assert!(
                        runtime
                            .block_on(dope_fiber::fiber!('_ => async move {
                                let ready = conn.observe().ready().await.expect("ready");
                                let channel = conn.channels().open(1).await.expect("channel");
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
                                let first = consumer
                                    .recv()
                                    .await
                                    .expect("consumer active")
                                    .expect("first delivery");
                                assert!(first.body() == b"a"[..]);
                                let first = first.settle(Disposition::Ack);

                                let second = consumer
                                    .recv()
                                    .await
                                    .expect("consumer active")
                                    .expect("second delivery");
                                assert!(second.body() == b"b"[..]);
                                first.await.expect("first settlement");
                                second
                                    .settle(Disposition::Ack)
                                    .await
                                    .expect("second settlement");
                                conn
                                    .control()
                                    .close(ready, 200, "OK")
                                    .await
                                    .expect("close");
                            }))
                            .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}
