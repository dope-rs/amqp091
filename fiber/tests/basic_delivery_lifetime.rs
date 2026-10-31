use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::Method;
use crate::common::cancel;
pub mod common;

use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dope::core::io::Reclaimed;
use dope::manifold::timing;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::ack::Disposition;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel;
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::harness::peer::TcpScript;

use crate::common::basic;
use crate::common::basic::delivery;

#[test]
fn outlives_consumer() {
    let done = AtomicBool::new(false);
    const QUEUE: &str = "official-q";
    const TAG: &str = "official-consumer";

    let done = &done;
    let signal = done;
    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            Method::read(stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");

            let consume = Method::read(stream).expect("basic.consume");
            delivery::check_consume::<ack::Manual>(&consume, 1, QUEUE, TAG)
                .expect("AMQP consumer method");
            let ok = delivery::consume_ok(TAG).expect("AMQP fixture");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("basic.consume-ok");
            stream
                .write_all(&delivery::encode(TAG).expect("AMQP fixture"))
                .expect("basic.deliver");

            let mut cancel = false;
            let mut ack_seen = false;
            while !cancel || !ack_seen {
                let method = Method::read(stream).expect("basic.cancel or basic.ack");
                match method.method() {
                    30 => {
                        cancel::check(&method, TAG).expect("AMQP consumer method");
                        stream
                            .write_all(
                                &Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"),
                            )
                            .expect("basic.cancel-ok");
                        cancel = true;
                    }
                    80 => {
                        method.check(1, 60, 80).expect("AMQP method route");
                        assert_eq!(method.arguments(), &[0, 0, 0, 0, 0, 0, 0, 1, 0]);
                        ack_seen = true;
                    }
                    other => assert!(matches!(other, 30 | 31 | 80), "unexpected Basic method"),
                }
            }
            signal.store(true, Ordering::Release);
        },
        |broker| {
            let channel = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(2)
                .returns(1);
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(channel)
                .egress(config::Egress::new(2, 4096));
            let config = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(capacity);

            {
                let mut reports = Vec::new();
                Endpoint::new(broker.addr(), config).enter::<timing::Throughput, _, _>(
                    basic::POLICY,
                    |runtime| {
                        let conn = runtime.connection();
                        let timer = runtime.timer();
                        assert!(runtime.block_on(dope_fiber::fiber!('_ => async move {
                    conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel open");
                    let arguments = Table::new();
                    let mut consumer = channel
                        .basic()
                        .consume(channel::basic::Consume {
                            queue: QUEUE,
                            consumer_tag: TAG,
                            no_local: false,
                            ack: ack::Manual,
                            exclusive: false,
                            arguments: &arguments,
                        })
                        .await
                        .expect("basic.consume");
                    assert_eq!(consumer.tag(), TAG);

                    let delivery = consumer
                        .recv()
                        .await
                        .expect("delivery state")
                        .expect("basic.deliver");
                    drop(consumer);
                    assert_eq!(delivery.consumer_tag(), TAG);
                    assert_eq!(delivery.exchange(), basic::EXCHANGE);
                    assert_eq!(delivery.routing_key(), basic::ROUTING_KEY);
                    assert_eq!(delivery.body().try_to_vec().expect("body copy"), basic::BODY);
                    let settled = delivery.settle(Disposition::Ack).await;
                    assert!(settled.is_ok(), "basic.ack");

                    while !done.load(Ordering::Acquire) {
                        let sleep = (timer.sleep(Duration::from_millis(1))).expect("cancel wait");
                        sleep.await;
                    }
                })).is_ok(), "AMQP fiber must complete");
                    },
                    |event| reports.push(event),
                    recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                assert!(
                    matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
                    "unexpected reclaimed AMQP completion: {reports:?}"
                );
            };
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

#[test]
fn empty_ends_at_header() {
    let done = AtomicBool::new(false);
    const QUEUE: &str = "official-empty-q";
    const TAG: &str = "official-empty-consumer";

    let done = &done;
    let signal = done;
    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            Method::read(stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            Method::write(stream, 1, 20, 11, &[0, 0, 0, 0]).expect("channel.open-ok");

            let consume = Method::read(stream).expect("basic.consume");
            delivery::check_consume::<ack::Auto>(&consume, 1, QUEUE, TAG)
                .expect("AMQP consumer method");
            let ok = delivery::consume_ok(TAG).expect("AMQP fixture");
            Method::write(stream, 1, 60, 21, &ok).expect("basic.consume-ok");
            stream
                .write_all(&delivery::empty(TAG).expect("AMQP fixture"))
                .expect("empty basic.deliver");

            let cancel = Method::read(stream).expect("basic.cancel");
            cancel::check(&cancel, TAG).expect("AMQP consumer method");
            Method::write(stream, 1, 60, 31, &ok).expect("basic.cancel-ok");
            signal.store(true, Ordering::Release);
        },
        |broker| {
            let channel = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(2)
                .returns(1);
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(channel)
                .egress(config::Egress::new(2, 4096));
            let config = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(capacity);

            {
                let mut reports = Vec::new();
                Endpoint::new(broker.addr(), config).enter::<timing::Throughput, _, _>(
                    basic::POLICY,
                    |runtime| {
                        let conn = runtime.connection();
                        let timer = runtime.timer();
                        assert!(runtime.block_on(dope_fiber::fiber!('_ => async move {
                    conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel open");
                    let arguments = Table::new();
                    let mut consumer = channel
                        .basic()
                        .consume(channel::basic::Consume {
                            queue: QUEUE,
                            consumer_tag: TAG,
                            no_local: false,
                            ack: ack::Auto,
                            exclusive: false,
                            arguments: &arguments,
                        })
                        .await
                        .expect("basic.consume");

                    let delivery = consumer
                        .recv()
                        .await
                        .expect("delivery state")
                        .expect("empty basic.deliver");
                    assert_eq!(delivery.delivery_tag(), 2);
                    assert!(delivery.body().is_empty());
                    assert_eq!(delivery.body().as_slice(), Some(&[][..]));
                    delivery.release();
                    drop(consumer);

                    while !done.load(Ordering::Acquire) {
                        let sleep = (timer.sleep(Duration::from_millis(1))).expect("cancel wait");
                        sleep.await;
                    }
                })).is_ok(), "AMQP fiber must complete");
                    },
                    |event| reports.push(event),
                    recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                assert!(
                    matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
                    "unexpected reclaimed AMQP completion: {reports:?}"
                );
            };
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
