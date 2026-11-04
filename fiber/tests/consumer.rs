use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::app::{Endpoint, Policy};
use crate::common::amqp::wire::{Method, put_short};
use crate::common::amqp::{Peer, Tune};
pub mod common;

use std::sync::mpsc;
use std::time::Duration;

use dope::core::io::Reclaimed;
use dope::manifold::timing;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091::{config, failure};
use dope_amqp091_fiber::channel::basic;
use dope_test::alloc::Tracker;
use dope_test::harness::peer::{AcceptScript, TcpScript};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

const POLICY: Policy = Policy::new(1, 8, Duration::from_secs(10));

#[test]
fn retains_collision() {
    let (done, wait) = mpsc::channel();
    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(Tune::new(1, 4096, 0))
                .expect("handshake");
            Method::read(stream).expect("channel").check(1, 20, 10).expect("AMQP method route");
            Method::write(stream, 1, 20, 11, &[0, 0, 0, 0]).expect("open reply");
            for tag in [b"\x06shared"; 2] {
                Method::read(stream).expect("consume").check(1, 60, 20).expect("AMQP method route");
                Method::write(stream, 1, 60, 21, tag).expect("consume reply");
            }
            Method::read(stream)
                .expect("connection close")
                .check(0, 10, 50).expect("AMQP method route");
            Method::write(stream, 0, 10, 51, &[]).expect("close reply");
            wait.recv_timeout(Duration::from_secs(10))
                .expect("retained failure");
        },
        |broker| {
            let config = config::Config::new("guest", "guest").expect("config");
            {
    let mut reports = Vec::new();
     Endpoint::new(broker.addr(), config)
                .enter::<timing::Throughput, _, _>(POLICY, |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let (error, again) = {
                            let channel = conn.channels().open(1).await.expect("channel");
                            let args = Table::new();
                            let first = channel.basic().consume(basic::Consume {
                                queue: "q", consumer_tag: "", no_local: false,
                                ack: ack::Auto, exclusive: false, arguments: &args,
                            }).await.expect("fresh consumer");
                            assert_eq!(first.tag(), "shared");
                            let error = channel.basic().consume(basic::Consume {
                                queue: "q", consumer_tag: "", no_local: false,
                                ack: ack::Auto, exclusive: false, arguments: &args,
                            }).await.err().expect("duplicate tag");
                            let again = conn.observe().ready().await.expect_err("terminal connection");
                            (error, again)
                        };
                        let (cause, next) = match (&error, &again) {
                            (failure::Runtime::Terminal(cause), failure::Runtime::Terminal(next)) => Some((cause, next)),
                            _ => None,
                        }.expect("shared terminal cause");
                        assert!(cause.is_same(next));
                        assert_eq!(cause.generation(), 1);
                        let tag = match cause.error() {
                            failure::Error::ConsumerCollision { tag, error: _ } => Some(tag),
                            _ => None,
                        }.expect("collision payload");
                        assert_eq!(tag.as_ref(), "shared");
                        drop(again);
                        assert!(matches!(error.error(), failure::Error::ConsumerCollision { tag, .. } if tag.as_ref() == "shared"));
                        done.send(()).expect("checked cause");
                    })).expect("fiber");
                }, |event| reports.push(event), recover, |result| {
                result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
            });
    assert!(matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]), "unexpected reclaimed AMQP completion: {reports:?}");
    };
            broker.finish().expect("broker completion");
        },
    ).expect("broker");
}

#[test]
fn retains_names() {
    let generated = "e".repeat(255);
    let expected = generated.as_str();
    let (done, wait) = mpsc::channel();
    let (ready, gate) = mpsc::channel();
    AcceptScript::with(
        move |acceptor| {
            for tag in [expected, "observed"] {
                let mut stream = acceptor.accept().expect("connection");
                stream
                    .read_wait(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                stream
                    .write_wait(Some(Duration::from_secs(10)))
                    .expect("write timeout");
                Peer::new(&mut stream)
                    .open(Tune::new(1, 4096, 0))
                    .expect("handshake");
                Method::read(&mut stream)
                    .expect("channel")
                    .check(1, 20, 10).expect("AMQP method route");
                Method::write(&mut stream, 1, 20, 11, &[0, 0, 0, 0]).expect("open reply");
                Method::read(&mut stream)
                    .expect("consume")
                    .check(1, 60, 20).expect("AMQP method route");
                let mut encoded = Vec::new();
                put_short(&mut encoded, tag).expect("consumer tag");
                Method::write(&mut stream, 1, 60, 21, &encoded).expect("consume reply");
                if tag == expected {
                    gate.recv_timeout(Duration::from_secs(10))
                        .expect("stored broker identity");
                } else {
                    Method::read(&mut stream)
                        .expect("connection close")
                        .check(0, 10, 50).expect("AMQP method route");
                    Method::write(&mut stream, 0, 10, 51, &[]).expect("close reply");
                }
            }
            wait.recv_timeout(Duration::from_secs(10))
                .expect("retained names");
        },
        |broker| {
            let config = config::Config::new("guest", "guest").expect("config");
            {
    let mut reports = Vec::new();
     Endpoint::new(broker.addr(), config)
                .enter::<timing::Throughput, _, _>(POLICY, |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let error = {
                            let channel = conn.channels().open(1).await.expect("channel");
                            let args = Table::new();
                            let mut consumer = channel.basic().consume(basic::Consume {
                                queue: "q", consumer_tag: "", no_local: false,
                                ack: ack::Auto, exclusive: false, arguments: &args,
                            }).await.expect("server-named consumer");
                            assert_eq!(consumer.tag(), expected);
                            ready.send(()).expect("begin replay");
                            consumer.recv().await.err().expect("mismatched replay tag")
                        };
                        let cause = match &error {
                            failure::Runtime::Terminal(cause) => Some(cause),
                            failure::Runtime::Local(_) => None,
                        }.expect("retained terminal cause");
                        assert_eq!(cause.generation(), 2);
                        let (retained, actual) = match cause.error() {
                            failure::Error::ConsumerTag { expected, actual } => Some((expected, actual)),
                            _ => None,
                        }.expect("both original text owners");
                        assert_eq!(retained.as_ref(), expected);
                        assert_eq!(actual.as_ref(), "observed");
                        assert!(retained.identity().is_some());
                        assert!(actual.identity().is_none());
                        let again = conn.observe().ready().await.expect_err("terminal connection");
                        let next = match &again {
                            failure::Runtime::Terminal(next) => Some(next),
                            failure::Runtime::Local(_) => None,
                        }.expect("same terminal cause");
                        assert!(cause.is_same(next));
                        let ((), counts) = Tracker::measure(|| {
                            drop(error);
                            assert!(matches!(again.error(), failure::Error::ConsumerTag { expected: retained, actual }
                                if retained.as_ref() == expected && actual.as_ref() == "observed"));
                            drop(again);
                        });
                        assert_eq!(counts, [0; 4]);
                        done.send(()).expect("checked names");
                    })).expect("fiber");
                }, |event| reports.push(event), recover, |result| {
                result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
            });
    assert!(matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]), "unexpected reclaimed AMQP completion: {reports:?}");
    };
            broker.finish().expect("broker completion");
        },
    ).expect("broker");
}

#[test]
fn close_wakes_consumer() {
    use std::cell::Cell;

    use dope_fiber::abi::PollFn;
    use dope_fiber::task::batch::{Join, Pool};
    use dope_test::fibers::Yield;

    use crate::common::basic::returned;
    use crate::common::fiber::Pinned;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(Tune::new(1, 4096, 0)).expect("handshake");
            assert_eq!(returned::open_channel(stream).expect("channel"), 1);
            Method::read(stream).expect("consume").check(1, 60, 20)
                .expect("consume method");
            Method::write(stream, 1, 60, 21, b"\x03old").expect("consume reply");
            Method::read(stream).expect("channel close").check(1, 20, 40)
                .expect("close method");
            Method::write(stream, 1, 20, 41, &[]).expect("channel close-ok");
            assert_eq!(returned::open_channel(stream).expect("reused channel"), 1);
            Method::read(stream).expect("reused channel close").check(1, 20, 40)
                .expect("close method");
            Method::write(stream, 1, 20, 41, &[]).expect("channel close-ok");
            returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            common::client::with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config")
                    .capacity(config::Capacity::default().channels(1)),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open_auto().await.expect("channel");
                        let arguments = Table::new();
                        let mut consumer = channel.basic().consume(basic::Consume {
                            queue: "q", consumer_tag: "old", no_local: false,
                            ack: ack::Manual, exclusive: false, arguments: &arguments,
                        }).await.expect("consumer");
                        let registered = Cell::new(false);
                        {
                            let channel = &channel;
                            let registered = &registered;
                            let waiters = [Some(&mut consumer), None].map(|consumer| dope_fiber::fiber!('_ => async move {
                                if let Some(consumer) = consumer {
                                    let mut receive = Pinned::new(consumer.recv());
                                    let result = PollFn::admitted(|admit| {
                                        let poll = admit.poll(receive.as_pin()).0;
                                        if poll.is_pending() {
                                            registered.set(true);
                                        }
                                        poll
                                    }).await;
                                    let error = result.err().expect("closed consumer");
                                    assert!(matches!(error.error(), failure::Error::Closed));
                                } else {
                                    while !registered.get() {
                                        Yield::new().await;
                                    }
                                    channel.close(200, "done").await.expect("channel close");
                                }
                            }));
                            let mut pool = Pool::<2>::acquire().await.expect("waiter tasks");
                            Join::from_array(&mut pool, waiters).await.expect("close wake");
                        }
                        let next = conn.channels().open_auto().await.expect("released channel slot");
                        assert_eq!(next.id(), 1);
                        assert_eq!(consumer.tag(), "old");
                        next.close(200, "done").await.expect("reused channel close");
                        conn.control().close(ready, 200, "done").await.expect("connection close");
                    })).expect("fiber");
                },
                recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("broker completion");
        },
    ).expect("broker");
}

#[test]
fn rejects_foreign_channel_key() {
    use dope::core::platform::wake;
    use dope_amqp091::adapter::{self, Poll as _};
    use dope_amqp091::protocol::method;
    use dope_amqp091::protocol::wire::text::short;
    use dope_fiber::abi::poll_fn;
    use dope_fiber::task::local::Once;
    use o3::base::cell::local;

    use crate::common::basic::returned;
    use crate::common::{cancel, fiber};

    let (cancelled, signal) = wake::Ends::blocking().expect("cancel pipe").split();
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(Tune::new(2, 4096, 0)).expect("handshake");
            for channel in [1, 2] {
                assert_eq!(returned::open_channel(stream).expect("channel"), channel);
            }
            for (channel, tag) in [(1, "first"), (2, "second")] {
                cancel::check_client_consume_on(&Method::read(stream).expect("consume"), channel, tag)
                    .expect("consumer route");
                let reply = cancel::consume_ok_arguments(tag).expect("consumer tag");
                Method::write(stream, channel, 60, 21, &reply).expect("consume reply");
            }
            stream.write_all(&cancel::delivery_frames_with_tag(2, "second", 7).expect("delivery"))
                .expect("queued delivery");
            cancel::check_on(&Method::read(stream).expect("rejected key cancellation"), 1, "first")
                .expect("original cancellation owner");
            Method::write(stream, 1, 60, 31, b"\x05first").expect("cancel reply");
            signal.notify().expect("cancel observed");
            let ack = Method::read(stream).expect("untouched delivery acknowledgement");
            ack.check(2, 60, 80).expect("settlement channel");
            assert_eq!(ack.arguments(), &[0, 0, 0, 0, 0, 0, 0, 7, 0]);
            returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            common::client::with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config")
                    .capacity(config::Capacity::default().channels(2)),
                |rt| {
                    let amqp = adapter::Adapter::new(rt.port());
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let mut leases = [None, None];
                        for (slot, id) in leases.iter_mut().zip(1..=2) {
                            let mut open = Once::new(|mut access| {
                                amqp.channels().reserve(access.reborrow(), ready, id)?
                                    .open(amqp, access, ready)
                            }).await.expect("channel reservation");
                            *slot = Some(poll_fn(|mut cx| {
                                let wake = cx.as_ref().completion();
                                open.poll(local::Access::new(cx.as_mut().region_token()), wake)
                            }).await.expect("channel open"));
                        }
                        let leases = leases.map(|lease| lease.expect("opened channel"));
                        let arguments = Table::new();
                        let mut keys = [None, None];
                        for ((slot, lease), tag) in keys.iter_mut().zip(leases).zip(["first", "second"]) {
                            let request = method::basic::Consume {
                                queue: short::Short::resource("q", short::Resource::Queue).expect("queue"),
                                consumer_tag: short::Short::new(tag).expect("consumer tag"),
                                no_local: false, ack: ack::Manual, exclusive: false, arguments: &arguments,
                            };
                            let mut call = Once::new(|access| {
                                amqp.methods().reserve(access, ready, lease, request)
                            }).await.expect("consume reservation");
                            let registered = poll_fn(|mut cx| {
                                let wake = cx.as_ref().completion();
                                call.poll(local::Access::new(cx.as_mut().region_token()), wake)
                            }).await.expect("consume");
                            *slot = Some(registered.key);
                        }
                        let [first, second] = keys.map(|key| key.expect("registered consumer"));
                        let error = amqp.consumers().try_bind(ready, leases[1], first)
                            .err().expect("foreign channel key");
                        assert!(matches!(error, failure::Error::Protocol(failure::wire::Fault::ConsumerLease {
                            expected, actual,
                        }) if expected != actual));
                        fiber::wait(&cancelled).await.expect("original key cancelled");
                        let mut consumer = amqp.consumers().try_bind(ready, leases[1], second)
                            .expect("matching channel key");
                        let delivery = {
                            let mut receive = consumer.recv();
                            poll_fn(|mut cx| {
                                let wake = cx.as_ref().waker();
                                receive.poll(local::Access::new(cx.as_mut().region_token()), || wake.completion())
                            }).await.expect("consumer receive").expect("untouched delivery")
                        };
                        assert_eq!(delivery.delivery_tag().get(), 7);
                        assert_eq!(delivery.body().as_slice(), Some(&b"x"[..]));
                        let mut settle = delivery.settle(ack::Disposition::Ack);
                        poll_fn(|mut cx| {
                            let wake = cx.as_ref().completion();
                            settle.poll(local::Access::new(cx.as_mut().region_token()), wake)
                        }).await.expect("matching delivery acknowledgement");
                        conn.control().close(ready, 200, "done").await.expect("connection close");
                    })).expect("fiber");
                },
                recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("broker completion");
        },
    ).expect("broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
