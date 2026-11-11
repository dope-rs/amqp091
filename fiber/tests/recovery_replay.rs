use crate::common::amqp::wire::{Frame, Method};
use crate::common::amqp::{Greeting, Peer, Tune};
use crate::common::recovery::client::complete as finish;
use crate::common::recovery::recover;
pub mod common;
use std::time::Duration;

use dope_test::harness::peer::AcceptScript;

#[test]
fn honors_negotiated_heartbeat_deadlines() {
    use crate::common::amqp::app::{Endpoint, Policy};
    use dope::manifold::timing;
    use dope_amqp091::{config, failure};
    use dope_test::harness::peer;
    use dope_test::harness::peer::TcpScript;

    for heartbeat in [0, 1, 2] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(Tune::new(16, 131_072, heartbeat)).expect("handshake");
                let started = std::time::Instant::now();
                let timeout = Duration::from_millis(u64::from(heartbeat.max(1)) * 750);
                stream.read_wait(Some(timeout)).expect("heartbeat read timeout");
                if heartbeat == 0 {
                    let error = stream.read(&mut [0]).expect_err("disabled heartbeat emitted a frame");
                    assert!(matches!(error, peer::Error::TimedOut) || matches!(error, peer::Error::Syscall(error) if error.is_block()));
                } else {
                    let frame = Frame::read(stream).expect("heartbeat before the full timeout");
                    frame.check(8, 0).expect("heartbeat route");
                    assert!(frame.payload().is_empty());
                }
                if heartbeat == 1 {
                    while stream.read(&mut [0; 256]).expect("inbound heartbeat timeout") != 0 {}
                    assert!(started.elapsed() < Duration::from_millis(1500), "peer silence must expire at one negotiated second");
                }
                if heartbeat != 1 {
                    Method::write(stream, 0, 10, 50, &[1, 147, 0, 0, 0, 0, 0])
                        .expect("heartbeat observation close");
                    while stream.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
                }
            },
            |broker| {
                let config = config::Config::new("guest", "guest").expect("config")
                    .tune(config::Tune::default().heartbeat(heartbeat));
                Endpoint::new(broker.addr(), config).shutdown(broker.completion()).try_check::<timing::Throughput, _, _>(
                    Policy::new(1, 16, Duration::from_secs(5)),
                    |rt| {
                        let conn = rt.connection();
                        rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            conn.observe().retired(ready.epoch()).await.expect("heartbeat retirement");
                            conn.observe().read(|view| {
                                let cause = view.last_exit().expect("heartbeat observation cause");
                                if heartbeat == 1 {
                                    assert_eq!(cause.reason(), failure::Reason::InboundTimeout);
                                } else {
                                    assert!(matches!(cause.error(), failure::Error::Server {
                                        reply_code: 403, class_id: 0, method_id: 0, ..
                                    }));
                                }
                            }).await;
                        })).expect("peer shutdown").expect("heartbeat drive");
                    },
                    recover,
                    finish,
                );
                broker.finish().expect("heartbeat peer");
            },
        ).expect("heartbeat peer");
    }
}

#[test]
fn readies_after_replay() {
    use dope::core::driver::schedule::{self, ready::wake::fanout};
    use dope::manifold::timing;
    use dope_amqp091::{adapter, config, failure};
    use dope_fiber::abi::poll_fn;

    use crate::common::amqp::app::{Endpoint, Policy};

    for complete in [false, true] {
        AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(Tune::new(16, 4096, 0))
                .expect("AMQP broker handshake");
            Method::read(&mut first)
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first Channel.OpenOk");
            Method::read(&mut first)
                .expect("initial topology observed")
                .check(1, 50, 30)
                .expect("queue.purge");
            drop(first);

            let mut second = acceptor.accept().expect("second connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            let mechanisms = "PLAIN ".repeat(700);
            let started = Peer::new(&mut second)
                .preface()
                .expect("replacement preface")
                .start(Greeting::new(0, 9, mechanisms.as_bytes(), b"en_US"))
                .expect("large replacement start")
                .recv()
                .expect("replacement start-ok");
            started
                .tune(Tune::new(16, 131_072, 1))
                .expect("replacement tune")
                .recv()
                .expect("replacement tune-ok")
                .recv()
                .expect("replacement open")
                .send()
                .expect("replacement open-ok");
            Method::read(&mut second)
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            let heartbeat = Frame::try_encode(8, 0, &[]).expect("AMQP heartbeat fixture");
            second
                .write_all(&heartbeat)
                .expect("broker heartbeat during replay");
            let frame = Frame::read(&mut second).expect("client heartbeat during replay");
            frame.check(8, 0).expect("heartbeat route");
            assert!(frame.payload().is_empty(), "heartbeat payload");
            if complete {
                let mut reply =
                    Method::try_encode(1, 20, 11, &[0; 4]).expect("AMQP frame fixture");
                reply.extend_from_slice(&heartbeat[..2]);
                second.write_all(&reply).expect("replayed Channel.OpenOk");
                // The next frame stays incomplete through the Ready observation.
            } else {
                Method::write(&mut second, 0, 10, 50, &[1, 147, 0, 0, 0, 0, 0])
                    .expect("close before replay reply");
            }
            while second.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        move |broker| {
            let config = config::Config::new("guest", "guest").expect("config");
            let policy = Policy::new(1, schedule::LIMIT, Duration::from_secs(10))
                .backoff(Duration::from_millis(10), Duration::from_secs(1));
            Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _, _>(
                policy,
                move |rt| {
                    let conn = rt.connection();
                    let amqp = adapter::Adapter::new(rt.port());
                    let mut subscriptions =
                        [const { fanout::Registration::new() }; schedule::LIMIT];

                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("first ready");
                            let channel = conn
                                .channels()
                                .open_in(target, 1)
                                .await
                                .expect("first channel");
                            let transport = conn.observe().read(|view| view.generation()).await;
                            let topology = conn
                                .observe()
                                .read(|view| view.ready_cycle())
                                .await;
                            assert_eq!(transport, 1);
                            assert_eq!(topology, Some(1));
                            // Keep a full application turn of subscribers through reconnection.
                            poll_fn(|cx| {
                                let wake = cx.as_ref().completion();
                                for subscription in &mut subscriptions {
                                    amqp.channels().watch_flow(subscription, &wake)
                                        .expect("flow subscription");
                                }
                                std::task::Poll::Ready(())
                            }).await;
                            assert!(channel.queue().purge("retained").await.is_err());
                            let ready = conn.observe().ready().await;
                            if complete {
                                ready.expect("ready before next frame completes");
                                conn.observe().read(|view| {
                                    assert_eq!(view.ready_cycle(), Some(2));
                                }).await;
                            } else {
                                let error = ready.expect_err("half-replayed topology must not be ready");
                                assert!(matches!(error.error(), failure::Error::Server {
                                    reply_code: 403, class_id: 0, method_id: 0, ..
                                }));
                                conn.observe().read(|view| {
                                    assert_eq!(view.generation(), 2);
                                    assert_eq!(view.ready_cycle(), None);
                                }).await;
                            }
                            drop(subscriptions);
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                finish,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
    }
}
