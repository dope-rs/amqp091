use std::task::Poll;

use dope_amqp091::config;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::{exchange, queue};
use dope_fiber::{abi::poll_fn, context};

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::Pinned;
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_amqp091::failure;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn reserves_inflight_slot() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            for channel in [1, 2] {
                Method::read(stream)
                    .expect("channel.open")
                    .check(channel, 20, 10)
                    .expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                            .expect("AMQP frame fixture"),
                    )
                    .expect("channel.open-ok");
            }

            Method::read(stream)
                .expect("exchange.declare")
                .check(1, 40, 10)
                .expect("AMQP method route");
            check_no_frame(stream, "reserved topology slot").expect("AMQP fixture");
            stream
                .write_all(&Method::try_encode(1, 40, 11, &[]).expect("AMQP frame fixture"))
                .expect("exchange.declare-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .topology(config::Topology::default().exchanges(1)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let first = conn.channels().open(1).await.expect("first channel");
                        let second = conn.channels().open(2).await.expect("second channel");
                        let args = Table::new();
                        let mut retained = Pinned::new(first.exchange().declare(exchange::Declare {
                            exchange: "retained",
                            kind: "direct",
                            durable: false,
                            auto_delete: false,
                            internal: false,
                            arguments: &args,
                        }));
                        let mut overflow = Pinned::new(second.exchange().declare(exchange::Declare {
                                exchange: "overflow",
                                kind: "direct",
                                durable: false,
                                auto_delete: false,
                                internal: false,
                                arguments: &args,
                            }));
                        poll_fn(|mut context| {
                            assert!(matches!(
                                context.as_mut().try_poll(retained.as_pin()),
                                context::PollResult::Polled(Poll::Pending)
                            ));
                            assert!(matches!(
                                context.as_mut().try_poll(overflow.as_pin()),
                                context::PollResult::Polled(Poll::Ready(Err(error)))
                                    if matches!(error.error(), failure::Error::Capacity(
                                        failure::wire::Capacity::TopologyMetadata
                                    ))
                            ));
                            Poll::Ready(())
                        }).await;
                        retained.await.expect("retained exchange");
                    })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn topology-reservation broker");
}

#[test]
fn retains_queue_slot() {
    TcpScript::with(
        move |stream| {
            assert!(
                Peer::new(stream).open(TUNE).is_ok(),
                "AMQP broker handshake"
            );
            for channel in [1, 2] {
                let method = (Method::read(stream)).expect("channel.open");
                method.check(channel, 20, 10).expect("AMQP method route");
                assert!(
                    stream
                        .write_all(
                            &Method::try_encode(channel, 20, 11, &[0; 4])
                                .expect("AMQP frame fixture")
                        )
                        .is_ok()
                );
            }
            let method = (Method::read(stream)).expect("reserved queue");
            method.check(1, 50, 10).expect("AMQP method route");
            assert_eq!(queue_name(&method).expect("queue name"), "retained");
            assert!(
                stream
                    .write_all(&queue_declare_ok_frame(1, b"retained").expect("AMQP fixture"))
                    .is_ok()
            );
            let method = (Method::read(stream)).expect("queue.delete");
            method.check(1, 50, 40).expect("AMQP method route");
            assert!(
                stream
                    .write_all(&Method::try_encode(1, 50, 41, &[0; 4]).expect("AMQP frame fixture"))
                    .is_ok()
            );
            let method = (Method::read(stream)).expect("reused queue slot");
            method.check(2, 50, 10).expect("AMQP method route");
            assert!(
                stream
                    .write_all(&queue_declare_ok_frame(2, b"reused").expect("AMQP fixture"))
                    .is_ok()
            );
        },
        |broker| {
            let config = (config::Config::new("guest", "guest")).expect("config");
            with(
                broker.addr(),
                config.capacity(
                    config::Capacity::default().topology(config::Topology::new(64).queues(1)),
                ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let first = (conn.channels().open(1).await).expect("first channel");
                        let second = (conn.channels().open(2).await).expect("second channel");
                        let args = Table::new();
                        let declare = |name| queue::Declare {
                            queue: name, durable: false, exclusive: false,
                            auto_delete: false, arguments: &args,
                        };
                        let name = "q".repeat(127);
                        let result = first.queue().declare(declare(&name)).await;
                        assert!(matches!(result,
                            Err(failure::Runtime::Local(failure::Error::Resident {
                                target: failure::wire::Allocation::Topology,
                                error: failure::wire::Resident::Budget { available: 64, .. },
                            }))
                        ));
                        let mut retained = Pinned::new(first.queue().declare(declare("retained")));
                        let mut overflow = Pinned::new(second.queue().declare(declare("overflow")));
                        poll_fn(|mut context| {
                            assert!(matches!(
                                context.as_mut().try_poll(retained.as_pin()),
                                context::PollResult::Polled(Poll::Pending)
                            ));
                            assert!(matches!(
                                context.as_mut().try_poll(overflow.as_pin()),
                                context::PollResult::Polled(Poll::Ready(Err(error)))
                                    if matches!(error.error(), failure::Error::Capacity(
                                        failure::wire::Capacity::TopologyMetadata
                                    ))
                            ));
                            Poll::Ready(())
                        }).await;
                        let result = retained.await;
                        assert!(result.is_ok(), "reserved queue succeeds");
                        let result = first.queue().delete("retained", false, false).await;
                        assert!(result.is_ok(), "remove queue");
                        let result = second.queue().declare(declare("reused")).await;
                        assert!(result.is_ok(), "reuse freed slot");
                    })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            assert!(broker.finish().is_ok(), "queue reservation broker");
        },
    )
    .expect("queue reservation broker");
}
