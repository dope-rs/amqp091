use std::env;

use crate::common::amqp::app::{Endpoint, Policy, vars};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use std::{process, time};

use dope::manifold::timing;
use dope_amqp091::adapter::conn::Ready;
use dope_amqp091::adapter::publish;
use dope_amqp091::message::ack::Disposition;
use dope_amqp091::message::field::Table;
use dope_amqp091::message::property::Properties;
use dope_amqp091::message::{Builder, ack};
use dope_amqp091::{config, protocol};
use dope_amqp091_fiber::channel::{basic, exchange, queue};
use dope_amqp091_fiber::conn::Connection;
use dope_fiber::task::time::sleep::TimerExt as _;

const POLICY: Policy = Policy::new(1, 16, time::Duration::from_secs(30));

#[test]
#[ignore = "requires an external RabbitMQ broker"]
fn roundtrip() {
    let endpoint = Endpoint::from_vars(vars::NAMES.map(env::var))
        .expect("RabbitMQ environment must be configured");
    const QUEUE_DOCUMENT: &str = "https://www.rabbitmq.com/docs/queues";
    const ACK_DOCUMENT: &str = "https://www.rabbitmq.com/docs/confirms";
    endpoint.try_check::<timing::Throughput, _, _>(
        POLICY,
        |rt| {
            let conn = rt.connection();
            let run = dope_fiber::fiber!('_ => async move {
                let ready = conn.observe().ready().await.expect(QUEUE_DOCUMENT);
                let chan = conn
                    .channels()
                    .open_in(ready, 1)
                    .await
                    .expect(QUEUE_DOCUMENT);
                let args = Table::new();
                let queue = chan
                    .queue()
                    .declare(queue::Declare {
                        queue: "",
                        durable: false,
                        exclusive: true,
                        auto_delete: true,
                        arguments: &args,
                    })
                    .await
                    .expect(QUEUE_DOCUMENT);
                chan
                    .queue()
                    .bind(
                        queue.queue.as_ref(),
                        "amq.direct",
                        queue.queue.as_ref(),
                        &args,
                    )
                    .await
                    .expect(QUEUE_DOCUMENT);
                chan.basic().qos(1).await.expect(QUEUE_DOCUMENT);
                let mut recv = chan
                    .basic()
                    .consume(basic::Consume {
                        queue: queue.queue.as_ref(),
                        consumer_tag: "documented-roundtrip",
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: true,
                        arguments: &args,
                    })
                    .await
                    .expect(QUEUE_DOCUMENT);
                let body = b"hello-from-test";
                let props = Builder::default()
                    .content_type("text/plain")
                    .expect(QUEUE_DOCUMENT)
                    .build();
                chan
                    .basic()
                    .publish(publish::Request {
                        exchange: "amq.direct",
                        routing_key: queue.queue.as_ref(),
                        mandatory: false,
                        properties: &props,
                        body,
                    })
                    .await
                    .expect(QUEUE_DOCUMENT);
                let msg = recv
                    .recv()
                    .await
                    .expect(QUEUE_DOCUMENT)
                    .expect(QUEUE_DOCUMENT);
                assert!(msg.body() == body[..], "{QUEUE_DOCUMENT}");
                msg
                    .settle(Disposition::Ack)
                    .await
                    .expect(ACK_DOCUMENT);
                conn
                    .control()
                    .start(ready, 200, "documented roundtrip complete")
                    .await
                    .expect(QUEUE_DOCUMENT);
            });
            assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
        },
        recover,
        complete,
    );
}

#[test]
#[ignore = "requires an external RabbitMQ broker"]
fn publisher_confirms_ack() {
    let endpoint = Endpoint::from_vars(vars::NAMES.map(env::var))
        .expect("RabbitMQ environment must be configured");
    const DOCUMENT: &str = "https://www.rabbitmq.com/docs/confirms";
    endpoint.try_check::<timing::Throughput, _, _>(
        POLICY,
        |rt| {
            let conn = rt.connection();
            let run = dope_fiber::fiber!('_ => async move {
                let ready = conn.observe().ready().await.expect(DOCUMENT);
                let chan = conn
                    .channels()
                    .open_in(ready, 1)
                    .await
                    .expect(DOCUMENT);
                let args = Table::new();
                let queue = chan
                    .queue()
                    .declare(queue::Declare {
                        queue: "",
                        durable: false,
                        exclusive: true,
                        auto_delete: true,
                        arguments: &args,
                    })
                    .await
                    .expect(DOCUMENT);
                chan
                    .queue()
                    .bind(
                        queue.queue.as_ref(),
                        "amq.direct",
                        queue.queue.as_ref(),
                        &args,
                    )
                    .await
                    .expect(DOCUMENT);
                let acks = chan.confirm().select().await.expect(DOCUMENT);
                let props = Properties::default();
                let reply = acks
                    .publish(publish::Request {
                        exchange: "amq.direct",
                        routing_key: queue.queue.as_ref(),
                        mandatory: false,
                        properties: &props,
                        body: b"confirm-me",
                    })
                    .await
                    .expect(DOCUMENT);
                assert_eq!(reply, protocol::Confirmation::Ack, "{DOCUMENT}");
                conn
                    .control()
                    .start(ready, 200, "documented publisher confirm complete")
                    .await
                    .expect(DOCUMENT);
            });
            assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
        },
        recover,
        complete,
    );
}

fn induce_disconnect(container: &str) {
    let output = process::Command::new("docker")
        .args([
            "exec",
            container,
            "rabbitmqctl",
            "close_all_connections",
            "amqp091-document-consumer-recovery",
        ])
        .output()
        .expect("run RabbitMQ remote-close command");
    assert!(
        output.status.success(),
        "RabbitMQ remote-close failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[dope_fiber::fiber_fn('d)]
async fn publish_reconnect<'o, 'r, 'c, 'b, 'app, 'd: 'app, B: ?Sized>(
    conn: Connection<'app, 'o, 'r, 'c, 'b, 'd, B, 0>,
    ready: Ready<'d, 0>,
    id: u16,
    body: &[u8],
) {
    const EXCHANGE: &str = "amqp091.document.consumer-recovery";
    const ROUTING_KEY: &str = "live.odds_change.1.sr:match:1";

    let chan = conn
        .channels()
        .open_in(ready, id)
        .await
        .expect("open publisher channel");
    let props = Properties::default();
    chan.basic()
        .publish(publish::Request {
            exchange: EXCHANGE,
            routing_key: ROUTING_KEY,
            mandatory: false,
            properties: &props,
            body,
        })
        .await
        .expect("publish recovery probe");
    chan.close(200, "publisher complete")
        .await
        .expect("close publisher channel");
}

#[test]
#[ignore = "requires an external RabbitMQ broker and AMQP_TEST_RECONNECT=1"]
fn recovers_consumer() {
    assert_eq!(
        env::var("AMQP_TEST_RECONNECT").as_deref(),
        Ok("1"),
        "set AMQP_TEST_RECONNECT=1 before running the disruptive integration test"
    );
    let container = env::var("RABBITMQ_TEST_CONTAINER")
        .expect("set RABBITMQ_TEST_CONTAINER to an isolated RabbitMQ test container");
    assert!(
        !container.trim().is_empty(),
        "the isolated test container must be named"
    );
    let endpoint = Endpoint::from_vars(vars::NAMES.map(env::var))
        .expect("RabbitMQ environment must be configured");
    const DOCUMENT: &str = "https://www.rabbitmq.com/docs/consumers#connection-recovery";
    const QUEUE_DOCUMENT: &str = "https://www.rabbitmq.com/docs/queues";
    endpoint
        .tune(config::Tune::default().heartbeat(2))
        .try_check::<timing::Throughput, _, _>(POLICY, |rt| {
            let conn = rt.connection();
            let timer = rt.timer();
            let run = dope_fiber::fiber!('_ => async move {
                let ready = conn.observe().ready().await.expect(DOCUMENT);
                let chan = conn
                    .channels()
                    .open_in(ready, 1)
                    .await
                    .expect(DOCUMENT);
                let args = Table::new();
                const EXCHANGE: &str = "amqp091.document.consumer-recovery";
                chan
                    .exchange()
                    .declare(exchange::Declare {
                        exchange: EXCHANGE,
                        kind: "topic",
                        durable: true,
                        auto_delete: false,
                        internal: false,
                        arguments: &args,
                    })
                    .await
                    .expect(DOCUMENT);
                let queue = chan
                    .queue()
                    .declare(queue::Declare {
                        queue: "",
                        durable: false,
                        exclusive: true,
                        auto_delete: true,
                        arguments: &args,
                    })
                    .await
                    .expect(QUEUE_DOCUMENT);
                assert!(!queue.queue.as_ref().is_empty(), "{QUEUE_DOCUMENT}");
                chan
                    .queue()
                    .bind(queue.queue.as_ref(), EXCHANGE, "#", &args)
                    .await
                    .expect(DOCUMENT);
                chan.basic().qos(50).await.expect(DOCUMENT);
                let mut recv = chan
                    .basic()
                    .consume(basic::Consume {
                        queue: queue.queue.as_ref(),
                        consumer_tag: "amqp091-reconnect-tag",
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: true,
                        arguments: &args,
                    })
                    .await
                    .expect(DOCUMENT);

                publish_reconnect(conn, ready, 20, b"generation-1").await;
                let first = recv
                    .recv()
                    .await
                    .expect(DOCUMENT)
                    .expect(DOCUMENT);
                assert!(first.epoch() == ready.epoch(), "{DOCUMENT}");
                assert!(first.body() == b"generation-1"[..], "{DOCUMENT}");
                first.settle(Disposition::Ack).await.expect(DOCUMENT);

                publish_reconnect(
                    conn,
                    ready,
                    21,
                    b"retired-generation-1",
                )
                .await;
                loop {
                    let snap = recv.snapshot().await.expect(DOCUMENT);
                    if snap.buffered_deliveries != 0 {
                        break;
                    }
                    timer
                        .sleep(time::Duration::from_millis(10))
                        .expect("generation-1 buffer wait")
                        .await;
                }
                induce_disconnect(&container);
                conn
                    .observe()
                    .retired(ready.epoch())
                    .await
                    .expect(DOCUMENT);
                let next = conn.observe().ready().await.expect(DOCUMENT);
                assert!(next.epoch() != ready.epoch(), "{DOCUMENT}");
                publish_reconnect(conn, next, 22, b"generation-2")
                    .await;
                let second = recv
                    .recv()
                    .await
                    .expect(DOCUMENT)
                    .expect(DOCUMENT);
                assert!(second.epoch() == next.epoch(), "{DOCUMENT}");
                assert!(
                    second.body() == b"generation-2"[..],
                    "{DOCUMENT} | {QUEUE_DOCUMENT}: a buffered delivery from the deleted queue must not be relabeled into the recovered generation"
                );
                second.settle(Disposition::Ack).await.expect(DOCUMENT);
                conn
                    .control()
                    .start(
                        next,
                        200,
                        "documented recovery complete",
                    )
                    .await
                    .expect(DOCUMENT);
            });
            assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
        }, recover, complete);
}
