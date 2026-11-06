use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;

pub mod common;
use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;

#[test]
fn replays_without_identity() {
    let tag = std::str::from_utf8(&[b't'; 255]).expect("ASCII consumer tag");

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
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first channel.open-ok");
            check_client_consume(&Method::read(&mut first).expect("AMQP method"), tag)
                .expect("AMQP consumer method");
            let ok = consume_ok_arguments(tag).expect("AMQP consumer tag");
            first
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("first basic.consume-ok");
            Method::read(&mut first)
                .expect("bound consumer purge")
                .check(1, 50, 30)
                .expect("consumer readiness route");
            Method::write(&mut first, 1, 50, 31, &0u32.to_be_bytes())
                .expect("consumer readiness reply");
            drop(first);

            let mut second = acceptor.accept().expect("recovered connection");
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
            check_client_consume(&Method::read(&mut second).expect("AMQP method"), tag)
                .expect("AMQP consumer method");
            second
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("replayed basic.consume-ok");
            second
                .write_all(&delivery_frames(1, tag).expect("AMQP fixture"))
                .expect("replayed delivery");
            check(&Method::read(&mut second).expect("AMQP method"), tag)
                .expect("AMQP cancel method");
            second
                .write_all(&Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"))
                .expect("replayed basic.cancel-ok");
            Method::write(&mut second, 0, 10, 50, &[0, 200, 0, 0, 0, 0, 0])
                .expect("retirement after replayed cancel-ok");
            Method::read(&mut second)
                .expect("connection.close-ok")
                .check(0, 10, 51)
                .expect("replayed cancellation processed");
        },
        move |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .topology(config::Topology::default().identities(0))
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let channel = conn
                            .channels().open_in(target, 1)
                            .await
                            .expect("first channel open");
                        let args = Table::new();
                        let mut consumer = channel
                            .basic()
                            .consume(basic::Consume {
                                queue: "q",
                                consumer_tag: tag,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("first basic.consume");
                        let tracked = conn
                            .observe()
                            .read(|view| view.channels())
                            .await;
                        assert_eq!(
                            tracked,
                            1,
                            "a consumer without a topology identity must still retain its channel",
                        );
                        channel.queue().purge("ready").await.expect("consumer readiness reply");
                        let delivery = consumer
                            .recv()
                            .await
                            .expect("replayed receive")
                            .expect("replayed delivery");
                        assert_eq!(delivery.consumer_tag(), tag);
                        delivery.release();
                        let recovered = conn.observe().ready().await.expect("replayed ready");
                        drop(consumer);
                        conn.observe().retired(recovered.epoch()).await.expect("replayed cancellation retired");
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
            broker.finish().expect("identity replay broker");
        },
    )
    .expect("identity replay broker");
}

#[test]
fn cancels_before_replay_is_ready() {
    use std::task::Poll;

    use dope::core::driver::schedule::{
        self,
        ready::wake::{fanout, queue},
    };
    use dope::manifold::timing;
    use dope_amqp091::{adapter, protocol};
    use dope_fiber::abi::poll_fn;
    use dope_fiber::task::local::Once;
    use dope_test::fibers::Yield;
    use o3::base::cell::local;

    use crate::common::amqp::app::{Endpoint, Policy};

    for (active, between, server_cancel) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
    ] {
        AcceptScript::with(
            move |acceptor| {
                let mut first = acceptor.accept().expect("first connection");
                first.read_wait(Some(Duration::from_secs(10))).expect("read deadline");
                Peer::new(&mut first).open(TUNE).expect("handshake");
                Method::read(&mut first).expect("open").check(1, 20, 10).expect("open route");
                Method::write(&mut first, 1, 20, 11, &[0; 4]).expect("open-ok");
                for tag in ["dropped", "kept"] {
                    check_client_consume(&Method::read(&mut first).expect("consume"), tag).expect("consume route");
                    Method::write(&mut first, 1, 60, 21, &consume_ok_arguments(tag).expect("tag")).expect("consume-ok");
                }
                Method::read(&mut first).expect("purge").check(1, 50, 30).expect("purge route");
                drop(first);

                let mut second = acceptor.accept().expect("recovered connection");
                second.read_wait(Some(Duration::from_secs(10))).expect("read deadline");
                Peer::new(&mut second).open(TUNE).expect("recovered handshake");
                Method::read(&mut second).expect("replay open").check(1, 20, 10).expect("open route");
                Method::write(&mut second, 1, 20, 11, &[0; 4]).expect("replay open-ok");
                check_client_consume(&Method::read(&mut second).expect("first replay"), "dropped").expect("first replay route");
                if active && !between {
                    Method::write(&mut second, 1, 60, 21, &consume_ok_arguments("dropped").expect("tag")).expect("first replay consume-ok");
                    check_client_consume(&Method::read(&mut second).expect("second replay"), "kept").expect("second replay route");
                }
                if server_cancel {
                    second.write_all(&frame(1, "dropped").expect("cancel")).expect("server cancellation");
                }
                Method::write(&mut second, 0, 10, 60, b"\x04drop").expect("drop barrier");
                Method::read(&mut second).expect("drop acknowledged").check(1, 60, 80).expect("ack route");
                let pending = if active && !between { "kept" } else { "dropped" };
                Method::write(&mut second, 1, 60, 21, &consume_ok_arguments(pending).expect("tag")).expect("pending replay consume-ok");
                check(&Method::read(&mut second).expect("replay cancellation"), "dropped").expect("cancel route");
                Method::write(&mut second, 1, 60, 31, &consume_ok_arguments("dropped").expect("tag")).expect("cancel-ok");
                if !active || between {
                    check_client_consume(&Method::read(&mut second).expect("retained replay"), "kept").expect("retained replay route");
                    Method::write(&mut second, 1, 60, 21, &consume_ok_arguments("kept").expect("tag")).expect("retained consume-ok");
                }
                Method::write(&mut second, 0, 10, 61, &[]).expect("unblocked");
                Method::read(&mut second).expect("ready purge").check(1, 50, 30).expect("ready purge route");
                Method::write(&mut second, 1, 50, 31, &[0; 4]).expect("ready purge-ok");
                Method::write(&mut second, 0, 10, 50, &[0, 200, 0, 0, 0, 0, 0]).expect("close");
                Method::read(&mut second).expect("close-ok").check(0, 10, 51).expect("close-ok route");
            },
            |broker| {
                let cfg = config::Config::new("guest", "guest").expect("config").capacity(
                    config::Capacity::default().channels(1).channel(
                        config::Channel::default().methods(1).consumers(2),
                    ),
                );
                Endpoint::new(broker.addr(), cfg).try_check::<timing::Throughput, _>(
                    Policy::new(1, schedule::LIMIT, Duration::from_secs(10)),
                    |rt| {
                        let conn = rt.connection();
                        let amqp = adapter::Adapter::new(rt.port());
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("first ready");
                            let channel = conn.channels().open_in(ready, 1).await.expect("channel").into_auto();
                            let args = Table::new();
                            let mut consumers = [None, None];
                            for (slot, tag) in consumers.iter_mut().zip(["dropped", "kept"]) {
                                *slot = Some(channel.basic().consume(basic::Consume {
                                    queue: "q", consumer_tag: tag, no_local: false,
                                    ack: ack::Manual, exclusive: false, arguments: &args,
                                }).await.expect("consume"));
                            }
                            assert!(channel.queue().purge("disconnect").await.is_err());
                            while !conn.observe().read(|view| view.is_blocked()).await {
                                Yield::new().await;
                            }
                            conn.observe().read(|view| {
                                assert_eq!(view.generation(), 2);
                                assert!(!view.is_ready());
                            }).await;
                            if !between {
                                drop(consumers[0].take());
                            }
                            let epoch = conn.observe().epoch().await.expect("recovery epoch");
                            let lease = Once::new(|access| amqp.channels().current(access, 1)).await.expect("channel lease");
                            let mut credit = queue::Registration::new();
                            let mut subscriptions = [const { fanout::Registration::new() }; schedule::LIMIT];
                            let mut armed = false;
                            poll_fn(|mut cx| {
                                if armed {
                                    return Poll::Ready(Ok(()));
                                }
                                let wake = cx.as_ref().completion();
                                if between {
                                    for subscription in &mut subscriptions {
                                        amqp.channels().watch_flow(subscription, &wake).expect("replay completion subscription");
                                    }
                                }
                                let access = local::Access::new(cx.as_mut().region_token());
                                let sent = amqp.channels().poll_send(access, epoch, lease,
                                    &protocol::method::basic::Ack { delivery_tag: 0, multiple: true },
                                    &mut credit, wake);
                                if between {
                                    assert!(matches!(sent, Poll::Ready(Ok(()))), "arming ack has no pending credit wake");
                                    armed = true;
                                    Poll::Pending
                                } else {
                                    sent
                                }
                            }).await.expect("drop barrier ack");
                            drop(subscriptions);
                            if between {
                                drop(consumers[0].take());
                            }
                            let recovered = conn.observe().ready().await.expect("ready after cancel-ok");
                            channel.queue().purge("ready").await.expect("ready purge");
                            conn.observe().retired(recovered.epoch()).await.expect("retired");
                            drop(consumers);
                        })).expect("replay cancellation drive");
                    },
                    recover,
                    |result| { result.as_ref().map_err(|error| error.kind()).expect("endpoint completion"); },
                );
                broker.finish().expect("replay cancellation broker");
            },
        ).expect("replay cancellation broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
