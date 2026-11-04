use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;

pub mod common;

use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;

#[test]
fn preserves_same_tag() {
    const TAG: &str = "same-tag";
    const CAPACITY_PROBE: &str = "capacity-probe";

    let done = AtomicBool::new(false);
    let done = &done;
    let peer = done;
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
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first channel.open-ok");

            check_client_consume(&Method::read(&mut first).expect("AMQP method"), TAG)
                .expect("AMQP consumer method");
            let tag_ok = consume_ok_arguments(TAG).expect("AMQP consumer tag");
            first
                .write_all(&Method::try_encode(1, 60, 21, &tag_ok).expect("AMQP frame fixture"))
                .expect("original basic.consume-ok");
            first
                .write_all(&frame(1, TAG).expect("AMQP fixture"))
                .expect("server cancel original");

            check_client_consume(&Method::read(&mut first).expect("AMQP method"), TAG)
                .expect("AMQP consumer method");
            first
                .write_all(&Method::try_encode(1, 60, 21, &tag_ok).expect("AMQP frame fixture"))
                .expect("replacement basic.consume-ok");

            check_client_consume(
                &Method::read(&mut first).expect("AMQP method"),
                CAPACITY_PROBE,
            )
            .expect("AMQP consumer method");
            let probe = consume_ok_arguments(CAPACITY_PROBE).expect("AMQP consumer tag");
            first
                .write_all(&Method::try_encode(1, 60, 21, &probe).expect("AMQP frame fixture"))
                .expect("capacity probe basic.consume-ok");
            first
                .write_all(&frame(1, CAPACITY_PROBE).expect("AMQP fixture"))
                .expect("server cancel capacity probe");

            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 60, 10)
                .expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 60, 11, &[]).expect("AMQP frame fixture"))
                .expect("qos barrier ok");
            drop(first);

            let mut second = broker.accept().expect("recovered connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("recovered channel.open-ok");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 60, 10)
                .expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 60, 11, &[]).expect("AMQP frame fixture"))
                .expect("recovered qos-ok");
            check_client_consume(&Method::read(&mut second).expect("AMQP method"), TAG)
                .expect("AMQP consumer method");
            second
                .write_all(&Method::try_encode(1, 60, 21, &tag_ok).expect("AMQP frame fixture"))
                .expect("replayed replacement basic.consume-ok");
            second
                .write_all(&delivery_frames(1, TAG).expect("AMQP fixture"))
                .expect("replacement delivery");
            check(&Method::read(&mut second).expect("AMQP method"), TAG)
                .expect("AMQP cancel method");
            second
                .write_all(&Method::try_encode(1, 60, 31, &tag_ok).expect("AMQP frame fixture"))
                .expect("replacement basic.cancel-ok");
            peer.store(true, Ordering::Release);
        },
        |broker| {
            let addr = broker.addr();

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
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                addr,
                config,
                |rt| {
                    let conn = rt.connection();
                    let timer = rt.timer();
                    let run = dope_fiber::fiber!('_ => async move {
                        let target = conn
                            .observe().ready()
                            .await
                            .expect("first ready");
                        let epoch = conn.observe().read(|view| view.generation()).await;
                        let chan = conn
                            .channels().open_in(target, 1)
                            .await
                            .expect("first channel open");
                        let args = Table::new();
                        let mut stale = chan
                            .basic()
                            .consume(basic::Consume {
                                queue: "q",
                                consumer_tag: TAG,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("original consumer");
                        let chan = chan.into_auto();
                        let end = stale.recv().await.expect("server cancellation");
                        assert!(end.is_none());
                        assert!(matches!(stale.recv().await, Ok(None)));

                        let mut next = chan
                            .basic()
                            .consume(basic::Consume {
                                queue: "q",
                                consumer_tag: TAG,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("same-tag replacement");
                        let (_, alloc) = Tracker::usage(|| drop(stale));
                        assert_eq!(alloc, (0, 0));

                        let mut probe = chan
                            .basic()
                            .consume(basic::Consume {
                                queue: "q",
                                consumer_tag: CAPACITY_PROBE,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("stale slot capacity reuse");
                        let end = probe
                            .recv()
                            .await
                            .expect("capacity probe server cancellation");
                        assert!(end.is_none());
                        let (_, alloc) = Tracker::usage(|| drop(probe));
                        assert_eq!(alloc, (0, 0));

                        chan
                            .basic()
                            .qos(1)
                            .await
                            .expect("server cancel dispatch barrier");
                        while conn.observe().read(|view| view.generation()).await == epoch {
                            timer
                                .sleep(Duration::from_millis(10))
                                .expect("generation wait")
                                .await;
                        }

                        let msg = next
                            .recv()
                            .await
                            .expect("replacement receive")
                            .expect("replacement delivery");
                        assert_eq!(msg.consumer_tag(), TAG);
                        assert!(msg.body() == b"x"[..]);
                        msg.release();
                        let (_, alloc) = Tracker::usage(|| drop(next));
                        assert_eq!(alloc, (0, 0));
                        while !done.load(Ordering::Acquire) {
                            timer
                                .sleep(Duration::from_millis(10))
                                .expect("replacement retirement wait")
                                .await;
                        }
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
            broker.finish().expect("same-tag replacement broker");
        },
    )
    .expect("same-tag replacement broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
