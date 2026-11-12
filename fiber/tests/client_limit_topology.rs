use dope_amqp091::config;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{basic, exchange, queue};

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short};
use crate::common::client::broker::rejection;

use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_amqp091::failure;
use dope_amqp091::message::ack;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn rejects_without_budget() {
    rejection(|broker| {
        with(
            broker.addr(),
            config::Config::new("guest", "guest")
                .expect("config")
                .capacity(
                    config::Capacity::default().topology(config::Topology::new(0).exchanges(1)),
                ),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let args = Table::new();
                        for exchange in ["retained-a", "retained-b"] {
                            let result = channel.exchange().declare(exchange::Declare {
                                    exchange,
                                    kind: "direct",
                                    durable: false,
                                    auto_delete: false,
                                    internal: false,
                                    arguments: &args,
                                }).await;
                            std::assert_matches!(result,
                                Err(error) if matches!((error).error(), failure::Error::Resident {
                                    target: failure::wire::Allocation::Topology,
                                    error: failure::wire::Resident::Budget {
                                        available: 0,
                                        ..
                                    },
                                })
                            );
                        }
                        channel.queue().purge("admission").await.expect("admission remains open");
                    }))
                    .is_ok(),
                    "AMQP fiber must complete"
                );
                broker.finish().expect("broker").expect("broker protocol");
            },
            recover,
            complete,
        );
    })
    .expect("bind topology broker");
}

#[test]
fn rejects_without_records() {
    rejection(|broker| {
        with(
            broker.addr(),
            config::Config::new("guest", "guest")
                .expect("config")
                .capacity(
                    config::Capacity::default().topology(config::Topology::default().exchanges(0)),
                ),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let args = Table::new();
                        let result = channel.exchange().declare(exchange::Declare {
                                exchange: "no-record-slot",
                                kind: "direct",
                                durable: false,
                                auto_delete: false,
                                internal: false,
                                arguments: &args,
                            }).await;
                        std::assert_matches!(result,
                            Err(error) if matches!((error).error(), failure::Error::Capacity(
                                failure::wire::Capacity::TopologyMetadata
                            ))
                        );
                        channel.queue().purge("admission").await.expect("admission remains open");
                    }))
                    .is_ok(),
                    "AMQP fiber must complete"
                );
                broker.finish().expect("broker").expect("broker protocol");
            },
            recover,
            complete,
        );
    })
    .expect("bind topology broker");
}

#[test]
fn rejects_without_identity() {
    rejection(|broker| {
        with(
            broker.addr(),
            config::Config::new("guest", "guest")
                .expect("config")
                .capacity(
                    config::Capacity::default().topology(config::Topology::default().identities(0)),
                ),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let args = Table::new();
                        let result = channel.queue().declare(queue::Declare {
                                queue: "",
                                durable: false,
                                exclusive: true,
                                auto_delete: false,
                                arguments: &args,
                            }).await;
                        assert!(matches!(result,
                            Err(error) if matches!((error).error(), failure::Error::Capacity(
                                failure::wire::Capacity::TopologyIdentity
                            ))
                        ));
                        channel.queue().purge("admission").await.expect("admission remains open");
                    }))
                    .is_ok(),
                    "AMQP fiber must complete"
                );
                broker.finish().expect("broker").expect("broker protocol");
            },
            recover,
            complete,
        );
    })
    .expect("bind topology broker");
}

#[test]
fn explicit_skips_identity() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");

            let declare = Method::read(stream).expect("AMQP method");
            declare.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&declare).expect("AMQP fixture"), "explicit-q");
            let mut ok = Vec::new();
            put_short(&mut ok, b"explicit-q").expect("AMQP short string");
            ok.extend_from_slice(&0u32.to_be_bytes());
            ok.extend_from_slice(&0u32.to_be_bytes());
            stream
                .write_all(&Method::try_encode(1, 50, 11, &ok).expect("AMQP frame fixture"))
                .expect("queue.declare-ok");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let mut ok = Vec::new();
            put_short(&mut ok, b"explicit-c").expect("AMQP short string");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("basic.consume-ok");
            Method::read(stream)
                .expect("connection close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .topology(config::Topology::default().identities(0)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let args = Table::new();
                            let declared = channel
                                .queue()
                                .declare(queue::Declare {
                                    queue: "explicit-q",
                                    durable: false,
                                    exclusive: false,
                                    auto_delete: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("explicit queue without identity slots");
                            assert_eq!(declared.queue.as_ref(), "explicit-q");

                            let consumer = channel
                                .basic()
                                .consume(basic::Consume {
                                    queue: "explicit-q",
                                    consumer_tag: "explicit-c",
                                    no_local: false,
                                    ack: ack::Manual,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("explicit consumer without identity slots");
                            assert_eq!(consumer.tag(), "explicit-c");
                            conn.control()
                                .close(ready, 200, "done")
                                .await
                                .expect("connection close");
                            drop(consumer);
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn explicit-topology broker");
}

#[test]
fn commits_generated_tag() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");

            let consume = Method::read(stream).expect("AMQP method");
            consume.check(1, 60, 20).expect("AMQP method route");
            let queue = consume.1[6] as usize;
            assert_eq!(consume.1[7 + queue], 0, "consumer tag was not empty");
            let mut ok = Vec::new();
            put_short(&mut ok, b"generated-c").expect("AMQP short string");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("basic.consume-ok");
            Method::read(stream)
                .expect("connection close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let args = Table::new();
                            let consumer = channel
                                .basic()
                                .consume(basic::Consume {
                                    queue: "q",
                                    consumer_tag: "",
                                    no_local: false,
                                    ack: ack::Manual,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("server-named consumer");
                            assert_eq!(consumer.tag(), "generated-c");
                            conn.control()
                                .close(ready, 200, "done")
                                .await
                                .expect("connection close");
                            drop(consumer);
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn server-named-consumer broker");
}

#[test]
fn rejects_generated_tag() {
    rejection(|broker| {
        with(
            broker.addr(),
            config::Config::new("guest", "guest")
                .expect("config")
                .capacity(
                    config::Capacity::default().topology(config::Topology::default().identities(0)),
                ),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let args = Table::new();
                        let result = channel.basic().consume(basic::Consume {
                                queue: "q",
                                consumer_tag: "",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            }).await;
                        assert!(matches!(result,
                            Err(error) if matches!((error).error(), failure::Error::Capacity(
                                failure::wire::Capacity::TopologyIdentity
                            ))
                        ));
                        channel.queue().purge("admission").await.expect("admission remains open");
                    }))
                    .is_ok(),
                    "AMQP fiber must complete"
                );
                broker.finish().expect("broker").expect("broker protocol");
            },
            recover,
            complete,
        );
    })
    .expect("bind topology broker");
}

#[test]
fn rejects_consumer_limit() {
    rejection(|broker| {
        let chan = config::Channel::default()
            .methods(1)
            .confirms(1)
            .consumers(0)
            .deliveries(1)
            .returns(1);
        with(
            broker.addr(),
            config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().channel(chan)),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let args = Table::new();
                        let result = channel.basic().consume(basic::Consume {
                                queue: "q",
                                consumer_tag: "no-slot",
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            }).await;
                        assert!(matches!(result,
                            Err(error) if matches!((error).error(), failure::Error::Capacity(
                                failure::wire::Capacity::Consumer
                            ))
                        ));
                        channel.queue().purge("admission").await.expect("admission remains open");
                    }))
                    .is_ok(),
                    "AMQP fiber must complete"
                );
                broker.finish().expect("broker").expect("broker protocol");
            },
            recover,
            complete,
        );
    })
    .expect("bind topology broker");
}

#[test]
fn rejects_ambiguous_queue() {
    rejection(|broker| {
        with(
            broker.addr(),
            config::Config::new("guest", "guest").expect("config"),
            |rt| {
                let conn = rt.connection();
                assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("open channel");
                        let args = Table::new();
                        let result = channel.queue().declare(queue::Declare {
                                queue: "",
                                durable: false,
                                exclusive: false,
                                auto_delete: false,
                                arguments: &args,
                            }).await;
                        assert!(matches!(result,
                            Err(error) if matches!(
                                error.error(), failure::Error::ServerNamedQueueNotExclusive
                            )
                        ));
                        channel.queue().purge("admission").await.expect("admission remains open");
                    }))
                    .is_ok(),
                    "AMQP fiber must complete"
                );
                broker.finish().expect("broker").expect("broker protocol");
            },
            recover,
            complete,
        );
    })
    .expect("bind topology broker");
}
