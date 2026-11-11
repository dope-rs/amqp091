use crate::common::amqp::Peer;
use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::{Frame, Method};
use crate::common::cancel;
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope::manifold::timing;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel;
use dope_test::fibers::Yield;
use dope_test::harness::peer::TcpScript;

use crate::common::basic;
use crate::common::basic::delivery;

#[test]
fn interleaves_by_channel() {
    const FIRST_QUEUE: &str = "official-first-q";
    const FIRST_TAG: &str = "official-first-consumer";
    const SECOND_QUEUE: &str = "official-second-q";
    const SECOND_TAG: &str = "official-second-consumer";

    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            let open = Method::read(stream).expect("first channel.open");
            let first = open.channel();
            open.check(first, 20, 10).expect("AMQP method route");
            Method::write(stream, first, 20, 11, &[0, 0, 0, 0]).expect("first channel.open-ok");

            let open = Method::read(stream).expect("second channel.open");
            let second = open.channel();
            open.check(second, 20, 10).expect("AMQP method route");
            assert_ne!(first, second);
            Method::write(stream, second, 20, 11, &[0, 0, 0, 0]).expect("second channel.open-ok");

            for (channel, queue, tag) in [
                (first, FIRST_QUEUE, FIRST_TAG),
                (second, SECOND_QUEUE, SECOND_TAG),
            ] {
                let consume = Method::read(stream).expect("basic.consume");
                delivery::check_consume::<ack::Auto>(&consume, channel, queue, tag)
                    .expect("AMQP consumer method");
                Method::write(
                    stream,
                    channel,
                    60,
                    21,
                    &delivery::consume_ok(tag).expect("AMQP fixture"),
                )
                .expect("basic.consume-ok");
            }

            stream
                .write_all(&delivery::method(first, FIRST_TAG, 1).expect("AMQP fixture"))
                .expect("first method");
            stream
                .write_all(&basic::content_header(first, 4).expect("AMQP fixture"))
                .expect("first header");
            stream
                .write_all(&Frame::try_encode(3, first, b"le").expect("AMQP frame fixture"))
                .expect("first partial body");
            stream
                .write_all(&delivery::method(second, SECOND_TAG, 2).expect("AMQP fixture"))
                .expect("second method");
            stream
                .write_all(&basic::content_header(second, 0).expect("AMQP fixture"))
                .expect("second empty header");
            stream
                .write_all(&Frame::try_encode(3, first, b"ft").expect("AMQP frame fixture"))
                .expect("first final body");

            basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(2)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(2)
                .channel(chan)
                .egress(config::Egress::new(4, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(cap);

            Endpoint::new(broker.addr(), cfg).try_check::<timing::Throughput, _, _>(
                basic::POLICY,
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let first = conn.channels().open(1).await.expect("first channel");
                            let second = conn.channels().open(2).await.expect("second channel");
                            let args = Table::new();
                            let mut first_consumer = first
                                .basic()
                                .consume(channel::basic::Consume {
                                    queue: FIRST_QUEUE,
                                    consumer_tag: FIRST_TAG,
                                    no_local: false,
                                    ack: ack::Auto,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("first consume");
                            let mut second_consumer = second
                                .basic()
                                .consume(channel::basic::Consume {
                                    queue: SECOND_QUEUE,
                                    consumer_tag: SECOND_TAG,
                                    no_local: false,
                                    ack: ack::Auto,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("second consume");

                            let second_delivery = second_consumer
                                .recv()
                                .await
                                .expect("second delivery state")
                                .expect("second delivery");
                            assert_eq!(second_delivery.delivery_tag().get(), 2);
                            assert!(second_delivery.body().is_empty());
                            second_delivery.release();

                            let first_delivery = first_consumer
                                .recv()
                                .await
                                .expect("first delivery state")
                                .expect("first delivery");
                            assert_eq!(first_delivery.delivery_tag().get(), 1);
                            assert_eq!(first_delivery.body().as_slice(), Some(&b"left"[..]));
                            first_delivery.release();
                            conn.control().close(ready, 200, "interleaved deliveries released")
                                .await.expect("connection close");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

#[test]
fn held_deliveries_block() {
    use dope_fiber::abi::race::{Either, Race};

    const QUEUE: &str = "official-backpressure-q";
    const TAG: &str = "official-backpressure-consumer";

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

            for (tag, byte) in [(1, b'a'), (2, b'b'), (3, b'c'), (4, b'd')] {
                stream
                    .write_all(&delivery::byte(TAG, tag, byte).expect("AMQP fixture"))
                    .expect("basic.deliver");
            }

            basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(2)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(cap);

            Endpoint::new(broker.addr(), cfg).try_check::<timing::Throughput, _, _>(
                basic::POLICY,
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel open");
                            let args = Table::new();
                            let mut consumer = channel
                                .basic()
                                .consume(channel::basic::Consume {
                                    queue: QUEUE,
                                    consumer_tag: TAG,
                                    no_local: false,
                                    ack: ack::Auto,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("basic.consume");

                            let mut held = Vec::with_capacity(3);
                            for tag in 1..=3 {
                                let delivery = consumer
                                    .recv()
                                    .await
                                    .expect("delivery state")
                                    .expect("basic.deliver");
                                assert_eq!(delivery.delivery_tag().get(), tag);
                                held.push(delivery);
                            }
                            let result = Race::new(consumer.recv(), Yield::new()).await;
                            assert!(matches!(result, Either::Right(())));
                            held.remove(0).release();

                            let fourth = consumer
                                .recv()
                                .await
                                .expect("fourth delivery state")
                                .expect("fourth basic.deliver");
                            assert_eq!(fourth.delivery_tag().get(), 4);
                            assert_eq!(fourth.body().as_slice(), Some(&b"d"[..]));
                            assert_eq!(held[0].body().as_slice(), Some(&b"b"[..]));
                            assert_eq!(held[1].body().as_slice(), Some(&b"c"[..]));
                            fourth.release();
                            for delivery in held {
                                delivery.release();
                            }
                            conn.control().close(ready, 200, "held deliveries released")
                                .await.expect("connection close");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

#[test]
fn cancel_wakes_route() {
    use dope_fiber::abi::race::{Either, Race};

    const FIRST_QUEUE: &str = "official-drain-first-q";
    const FIRST_TAG: &str = "official-drain-first-consumer";
    const SECOND_QUEUE: &str = "official-drain-second-q";
    const SECOND_TAG: &str = "official-drain-second-consumer";

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

            for (queue, tag) in [(FIRST_QUEUE, FIRST_TAG), (SECOND_QUEUE, SECOND_TAG)] {
                let consume = Method::read(stream).expect("basic.consume");
                delivery::check_consume::<ack::Auto>(&consume, 1, queue, tag)
                    .expect("AMQP consumer method");
                Method::write(
                    stream,
                    1,
                    60,
                    21,
                    &delivery::consume_ok(tag).expect("AMQP fixture"),
                )
                .expect("basic.consume-ok");
            }

            stream
                .write_all(&delivery::byte(FIRST_TAG, 1, b'a').expect("AMQP fixture"))
                .expect("first basic.deliver");
            stream
                .write_all(&delivery::byte(SECOND_TAG, 2, b'b').expect("AMQP fixture"))
                .expect("second basic.deliver");
            let cancel = Method::read(stream).expect("basic.cancel");
            cancel::check(&cancel, FIRST_TAG).expect("AMQP consumer method");
            Method::write(
                stream,
                1,
                60,
                31,
                &delivery::consume_ok(FIRST_TAG).expect("AMQP fixture"),
            )
            .expect("basic.cancel-ok");
            Method::read(stream)
                .expect("cancellation barrier")
                .check(1, 50, 30)
                .expect("queue.purge");
            Method::write(stream, 1, 50, 31, &0u32.to_be_bytes()).expect("queue.purge-ok");
            basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(2)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(cap);

            Endpoint::new(broker.addr(), cfg).try_check::<timing::Throughput, _, _>(
                basic::POLICY,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                let ready = conn.observe().ready().await.expect("ready");
                let channel = conn.channels().open(1).await.expect("channel open");
                let args = Table::new();
                let first = channel
                    .basic()
                    .consume(channel::basic::Consume {
                        queue: FIRST_QUEUE,
                        consumer_tag: FIRST_TAG,
                        no_local: false,
                        ack: ack::Auto,
                        exclusive: false,
                        arguments: &args,
                    })
                    .await
                    .expect("first basic.consume");
                let mut second = channel
                    .basic()
                    .consume(channel::basic::Consume {
                        queue: SECOND_QUEUE,
                        consumer_tag: SECOND_TAG,
                        no_local: false,
                        ack: ack::Auto,
                        exclusive: false,
                        arguments: &args,
                    })
                    .await
                    .expect("second basic.consume");

                loop {
                    let snap = first.snapshot().await.expect("first consumer snapshot");
                    assert_eq!(snap.queue.as_ref(), FIRST_QUEUE);
                    if snap.buffered_deliveries == 1 {
                        break;
                    }
                    Yield::new().await;
                }
                let snapshot = second.snapshot().await.expect("second consumer snapshot");
                assert_eq!(snapshot.queue.as_ref(), SECOND_QUEUE);
                assert_eq!(snapshot.buffered_deliveries, 0);
                let result = Race::new(second.recv(), Yield::new()).await;
                assert!(matches!(result, Either::Right(())));

                drop(first);
                let delivery = second
                    .recv()
                    .await
                    .expect("second delivery state")
                    .expect("second basic.deliver");
                assert_eq!(delivery.consumer_tag(), SECOND_TAG);
                assert_eq!(delivery.delivery_tag().get(), 2);
                assert_eq!(delivery.body().as_slice(), Some(&b"b"[..]));
                delivery.release();

                let result = Race::new(second.recv(), Yield::new()).await;
                assert!(matches!(result, Either::Right(())));
                // Receiving the second delivery proves retirement freed the only route;
                // its Cancel is already queued before this method can be admitted.
                channel.queue().purge("canceled").await.expect("cancellation barrier");
                conn.control().close(ready, 200, "canceled route released")
                    .await.expect("connection close");
            })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}
