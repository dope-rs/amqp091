use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::wire::{Frame, Method};
use crate::common::amqp::{Peer, Tune};
pub mod common;
use std::io::{self, Write as _};
use std::time::Duration;

use dope::core::platform::wake;
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::harness::peer::AcceptScript;

use crate::common::fiber::wait;
use crate::common::recovery::TUNE;
use crate::common::recovery::client::with;

#[test]
fn honors_negotiated_heartbeat_deadlines() {
    use crate::common::amqp::app::{Endpoint, Policy};
    use dope::manifold::timing;
    use dope_amqp091::{config, failure};
    use dope_test::harness::peer::TcpScript;
    use std::io::{ErrorKind, Read as _};

    for heartbeat in [0, 1, 2] {
        let (observed, observe) = wake::Ends::blocking()
            .expect("heartbeat observation")
            .split();
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(Tune::new(16, 131_072, heartbeat)).expect("handshake");
                let started = std::time::Instant::now();
                let timeout = Duration::from_millis(u64::from(heartbeat.max(1)) * 750);
                stream.read_wait(Some(timeout)).expect("heartbeat read timeout");
                if heartbeat == 0 {
                    let error = stream.read(&mut [0]).expect_err("disabled heartbeat emitted a frame");
                    assert!(matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut));
                } else {
                    let frame = Frame::read(stream).expect("heartbeat before the full timeout");
                    frame.check(8, 0).expect("heartbeat route");
                    assert!(frame.payload().is_empty());
                }
                if heartbeat == 1 {
                    io::copy(stream, &mut io::sink()).expect("inbound heartbeat timeout");
                    assert!(started.elapsed() < Duration::from_millis(1500), "peer silence must expire at one negotiated second");
                }
                observe.notify().expect("heartbeat observed");
                if heartbeat != 1 {
                    io::copy(stream, &mut io::sink()).expect("endpoint teardown");
                }
            },
            |broker| {
                let config = config::Config::new("guest", "guest").expect("config")
                    .tune(config::Tune::default().heartbeat(heartbeat));
                Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _>(
                    Policy::new(1, 16, Duration::from_secs(5)),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            if heartbeat == 1 {
                                conn.observe().retired(ready.epoch()).await.expect("heartbeat retirement");
                                conn.observe().read(|view| {
                                    let cause = view.last_exit().expect("heartbeat timeout cause");
                                    assert_eq!(cause.reason(), failure::Reason::InboundTimeout);
                                }).await;
                            }
                            wait(&observed).await.expect("heartbeat observation");
                            if heartbeat == 0 {
                                conn.observe().read(|view| assert!(view.ready() == Some(ready))).await;
                            }
                        })).expect("heartbeat drive");
                    },
                    recover,
                    |result| { result.as_ref().map_err(|error| error.kind()).expect("endpoint completion"); },
                );
                broker.finish().expect("heartbeat peer");
            },
        ).expect("heartbeat peer");
    }
}

#[test]
fn readies_after_replay() {
    let (released, release) = wake::Ends::blocking().expect("replay release").split();
    let (observed, observe) = wake::Ends::blocking().expect("replay observation").split();
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
                .open(TUNE)
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
            released
                .wait_timeout(Duration::from_secs(10))
                .expect("initial topology observed");
            drop(first);

            let mut second = acceptor.accept().expect("second connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(Tune::new(16, 131_072, 1))
                .expect("AMQP broker handshake");
            Method::read(&mut second)
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            observe.notify().expect("replay request observed");
            released
                .wait_timeout(Duration::from_secs(10))
                .expect("partial replay observed");
            second
                .write_all(&Frame::try_encode(8, 0, &[]).expect("AMQP heartbeat fixture"))
                .expect("broker heartbeat during replay");
            let frame = Frame::read(&mut second).expect("client heartbeat during replay");
            frame.check(8, 0).expect("heartbeat route");
            assert!(frame.payload().is_empty(), "heartbeat payload");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("replayed Channel.OpenOk");
            io::copy(&mut second, &mut io::sink()).expect("endpoint teardown");
        },
        move |broker| {
            with(
                broker.addr(),
                move |rt| {
                    let conn = rt.connection();
                    let timer = rt.timer();

                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("first ready");
                            let _channel = conn
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
                            release.notify().expect("initial topology observed");

                            wait(&observed).await.expect("replay request observed");
                            let transport = conn.observe().read(|view| view.generation()).await;
                            let topology = conn
                                .observe()
                                .read(|view| view.ready_cycle())
                                .await;
                            assert_eq!(transport, 2);
                            assert_eq!(
                                topology, None,
                                "transport readiness must not certify half-replayed topology"
                            );
                            release.notify().expect("partial replay observed");
                            while conn
                                .observe()
                                .read(|view| view.ready_cycle())
                                .await
                                != Some(2)
                            {
                                timer.sleep(Duration::from_millis(1)).expect("topology wait").await;
                            }
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
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
