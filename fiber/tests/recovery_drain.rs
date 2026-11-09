use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::client::queue_name;
use crate::common::recovery::broker::{consume_ok, queue_declare_ok, tagged};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use std::time::Duration;

use dope_amqp091::message::ack;
use dope_amqp091::message::ack::Disposition;
use dope_amqp091::message::field::Table;
use dope_amqp091::{config, failure};
use dope_amqp091_fiber::channel::{basic, queue};
use dope_test::fibers::Yield;
use dope_test::harness::peer::{AcceptScript, Stream};

use crate::common::recovery::TUNE;
use crate::common::recovery::client::with_config;

fn open_topology(mut stream: &Stream<'_>, name: &str) {
    Method::read(&mut stream)
        .expect("Channel.Open")
        .check(1, 20, 10)
        .expect("AMQP method route");
    stream
        .write_all(&Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"))
        .expect("Channel.OpenOk");
    let decl = Method::read(&mut stream).expect("Queue.Declare");
    decl.check(1, 50, 10).expect("AMQP method route");
    assert_eq!(queue_name(&decl).expect("queue name"), "");
    stream
        .write_all(&queue_declare_ok(1, name, 0, 1).expect("queue declaration"))
        .expect("Queue.DeclareOk");
    let sub = Method::read(&mut stream).expect("Basic.Consume");
    sub.check(1, 60, 20).expect("AMQP method route");
    assert_eq!(queue_name(&sub).expect("queue name"), name);
    stream
        .write_all(&consume_ok(1, "tag").expect("consumer tag"))
        .expect("Basic.ConsumeOk");
}

#[test]
fn follows_active_queue() {
    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("first generation");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            open_topology(&first, "q1");
            let query = Method::read(&mut first).expect("first queue fence");
            query.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&query).expect("queue name"), "q1");
            drop(first);

            let mut second = broker.accept().expect("second generation");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            open_topology(&second, "q2");
            second
                .write_all(&tagged(1, &[]).expect("AMQP delivery"))
                .expect("delivery");
            let ack = Method::read(&mut second).expect("Basic.Ack");
            ack.check(1, 60, 80).expect("AMQP method route");
            let query = Method::read(&mut second).expect("Queue.Declare");
            query.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&query).expect("queue name"), "q2");
            let flags = query.1[7 + queue_name(&query).expect("queue name").len()];
            assert_ne!(flags & 0b1, 0, "queue declare must be passive");
            second
                .write_all(&queue_declare_ok(1, "q2", 0, 1).expect("queue declaration"))
                .expect("passive Queue.DeclareOk");
            Method::read(&mut second)
                .expect("second fence released")
                .check(1, 50, 30)
                .expect("queue.purge");
            drop(second);

            let mut third = broker.accept().expect("third generation");
            third
                .read_wait(Some(Duration::from_secs(10)))
                .expect("third read timeout");
            third
                .write_wait(Some(Duration::from_secs(10)))
                .expect("third write timeout");
            Peer::new(&mut third)
                .open(TUNE)
                .expect("AMQP broker handshake");
            open_topology(&third, "q3");
            let query = Method::read(&mut third).expect("third queue fence");
            query.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&query).expect("queue name"), "q3");
            third
                .write_all(&queue_declare_ok(1, "q3", 0, 1).expect("queue declaration"))
                .expect("passive Queue.DeclareOk");
            Method::read(&mut third)
                .expect("close after third queue fence")
                .check(0, 10, 50)
                .expect("connection.close");
            Method::write(&mut third, 0, 10, 51, &[]).expect("connection.close-ok");
        },
        |broker| {
            let addr = broker.addr();

            with_config(
                addr,
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .topology(config::Topology::default().queues(1).identities(2)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    let run = dope_fiber::fiber!('_ => async move {
                        let target = conn
                            .observe()
                            .ready()
                            .await
                            .expect("ready");
                        let chan = conn
                            .channels()
                            .open_in(target, 1)
                            .await
                            .expect("channel");
                        let empty = Table::new();
                        let decl = chan
                            .queue()
                            .declare(queue::Declare {
                                queue: "",
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &empty,
                            })
                            .await
                            .expect("server-named queue");
                        assert_eq!(decl.queue.as_ref(), "q1");
                        let mut sub = chan
                            .basic()
                            .consume(basic::Consume {
                                queue: decl.queue.as_ref(),
                                consumer_tag: "tag",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: true,
                                arguments: &empty,
                            })
                            .await
                            .expect("consumer");
                        let initial_queue = Box::<str>::from(decl.queue.as_ref());
                        drop(decl);
                        let chan = chan.into_auto();
                        let error = chan
                            .in_ready(target)
                            .queue()
                            .inspect(queue::Inspect {
                                queue: &initial_queue,
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &empty,
                            })
                            .await
                            .err()
                            .expect("in-flight fence must fail when its epoch retires");
                        match &error {
                            failure::Runtime::Terminal(cause) => {
                                assert_eq!(cause.generation(), 1);
                            }
                            failure::Runtime::Local(cause) => {
                                std::assert_matches!(cause, failure::Error::Closed);
                            }
                        }
                        drop(error);

                        conn.observe().retired(target.epoch()).await.expect("first retirement");
                        let current = conn.observe().ready().await.expect("second ready");
                        let error = chan
                            .in_ready(target)
                            .queue()
                            .inspect(queue::Inspect {
                                queue: &initial_queue,
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &empty,
                            })
                            .await
                            .err()
                            .expect("retired fence must not reach the recovered transport");
                        std::assert_matches!(error.error(), failure::Error::Closed);
                        drop(error);
                        let snap = loop {
                            let snap = sub.snapshot().await.expect("consumer snapshot");
                            if snap.buffered_deliveries == 1 {
                                break snap;
                            }
                            Yield::new().await;
                        };
                        assert_eq!(snap.generation, 2);
                        assert_eq!(snap.queue.as_ref(), "q2");
                        drop(snap);

                        let item = sub
                            .recv()
                            .await
                            .expect("receive delivery")
                            .expect("delivery");
                        item
                            .settle(Disposition::Ack)
                            .await
                            .expect("generation-bound ACK admission");
                        let prior = sub.snapshot().await.expect("pre-fence snapshot");
                        assert_eq!(prior.buffered_deliveries, 0);
                        let queue_name = Box::<str>::from(prior.queue.as_ref());
                        drop(prior);
                        let fence = chan
                            .in_ready(current)
                            .queue()
                            .inspect(queue::Inspect {
                                queue: &queue_name,
                                durable: false,
                                exclusive: true,
                                auto_delete: true,
                                arguments: &empty,
                            })
                            .await
                            .expect("same-channel settlement fence");
                        assert_eq!(fence.message_count, 0);
                        drop(fence);
                        chan.in_ready(current).queue().purge(&queue_name).await.expect_err("second transport closes");
                        conn.observe().retired(current.epoch()).await.expect("second retirement");
                        let current = conn.observe().ready().await.expect("third ready");
                        let next = sub.snapshot().await.expect("third snapshot");
                        assert_eq!(next.generation, 3);
                        assert_eq!(next.queue.as_ref(), "q3");
                        assert_eq!(initial_queue.as_ref(), "q1");
                        let queue_name = Box::<str>::from(next.queue.as_ref());
                        drop(next);
                        assert_eq!(chan.in_ready(current).queue().inspect(queue::Inspect {
                            queue: &queue_name,
                            durable: false,
                            exclusive: true,
                            auto_delete: true,
                            arguments: &empty,
                        }).await.expect("third same-channel fence").message_count, 0);
                        conn.control().close(current, 200, "done").await.expect("close after third queue fence");
                    });
                    assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted broker");
        },
    )
    .expect("scripted broker");
}
