use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_test::alloc;
use dope_test::harness::support::gates::Barrier;

use crate::common::amqp::Peer;
use crate::common::amqp::app::{Endpoint, Policy};
use crate::common::amqp::wire::Method;
pub mod common;

use std::num::NonZero;
use std::time::Duration;

use common::wake::Purge;
use dope::core::driver::schedule::ready::wake::fanout;
use dope::core::io::Reclaimed;
use dope::core::platform::wake;
use dope::manifold::timing;
use dope_amqp091::adapter::Poll as _;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091::protocol::method::{basic, channel, queue};
use dope_amqp091::protocol::wire::text::short;
use dope_amqp091::{adapter, config, failure};
use dope_fiber::abi::poll_fn;
use dope_fiber::task::local::Once;
use dope_test::alloc::Tracker;
use dope_test::fibers::Yield;
use dope_test::harness::peer::{AcceptScript, TcpScript};
use o3::base::cell::local;

use crate::common::cancel::{check, check_client_consume, consume_ok_arguments};
use crate::common::fiber::wait;
use crate::common::wake::{Fault, Open, Ready, TUNE, with_client};

#[test]
fn keeps_generation() {
    let (open, notify) = wake::Ends::blocking().expect("arrival pipe").split();
    let gate = Barrier::new(NonZero::<usize>::MIN, Some(notify));
    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("generation-1 connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("generation-1 read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("generation-1 write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("generation-1 handshake");
            Method::read(&mut first)
                .expect("generation-1 channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("generation-1 channel.open-ok");
            open.wait_timeout(Duration::from_secs(10))
                .expect("test gate");
            drop(first);

            let mut second = broker.accept().expect("generation-2 connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("generation-2 read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("generation-2 write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("generation-2 handshake");
            Method::read(&mut second)
                .expect("generation-2 channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("generation-2 channel.open-ok");
            Method::read(&mut second)
                .expect("fresh queue.purge")
                .check(1, 50, 30)
                .expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 50, 31, &0_u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("fresh queue.purge-ok");
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                16,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let first = ((scope.block_on(Ready {
                        state: amqp.connection().observe(),
                        wait: fanout::Registration::new(),
                        gate: None,
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let lease = ((scope.block_on(Open {
                        adapter: amqp,
                        target: first,
                        lease: None,
                        call: None,
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let guard = ((scope.block_on(Once::new(move |local| {
                        amqp.methods().reserve(local, first, lease, channel::Open)
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let mut stale = ((scope.block_on(Once::new(move |local| {
                        let (result, alloc) = Tracker::usage(|| {
                            amqp.methods().reserve(
                                local,
                                first,
                                lease,
                                queue::Purge {
                                    queue: short::Name::EMPTY,
                                },
                            )
                        });
                        assert_eq!(alloc, (0, 0));
                        result
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let error = ((scope.block_on(Purge {
                        call: &mut stale,
                        gate: Some(&gate),
                    }))
                    .expect("queued method survived generation 1"))
                    .err()
                    .expect("queued method survived generation 1");
                    let (cause,) = match error {
                        Fault::Runtime(failure::Runtime::Terminal(cause)) => Some((cause,)),
                        _ => None,
                    }
                    .expect("queued method retirement lost its terminal cause");
                    assert_eq!(cause.generation(), 1);
                    drop(guard);

                    let second = ((scope.block_on(Ready {
                        state: amqp.connection().observe(),
                        wait: fanout::Registration::new(),
                        gate: None,
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let mut fresh = ((scope.block_on(Once::new(move |local| {
                        let (result, alloc) = Tracker::usage(|| {
                            amqp.methods().reserve(
                                local,
                                second,
                                lease,
                                queue::Purge {
                                    queue: short::Name::EMPTY,
                                },
                            )
                        });
                        assert_eq!(alloc, (0, 0));
                        result
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let reply = ((scope.block_on(Purge {
                        call: &mut fresh,
                        gate: None,
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    assert_eq!(reply.message_count, 0);
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker
                .finish()
                .expect("method generation broker completion");
        },
    )
    .expect("method generation broker");
}

#[test]
fn closes_abandoned_open_before_new_method() {
    let (finished, complete) = wake::Ends::blocking().expect("close completion").split();
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream).expect("channel open").check(1, 20, 10).expect("open route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream).expect("abandoned channel close").check(1, 20, 40).expect("close route");
            Method::write(stream, 1, 20, 41, &[]).expect("close reply");
            finished.wait_timeout(Duration::from_secs(5)).expect("channel collected");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().channel(config::Channel::default().methods(1)));
            Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _>(
                Policy::new(1, 16, Duration::from_secs(10)),
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let target = scope.block_on(Ready {
                        state: amqp.connection().observe(),
                        wait: fanout::Registration::new(),
                        gate: None,
                    }).expect("fiber drive").expect("connection ready");
                    let (lease, mut opening) = scope.block_on(Once::new(|mut access| {
                        let lease = amqp.channels().reserve(access.reborrow(), target, 1)?.commit();
                        amqp.methods()
                            .reserve(access, target, lease, channel::Open)
                            .map(|opening| (lease, opening))
                    })).expect("fiber drive").expect("open reservation");
                    scope.block_on(poll_fn(|mut cx| {
                        let wake = cx.as_ref().completion();
                        let access = local::Access::new(cx.as_mut().region_token());
                        assert!(opening.poll(access, wake).is_pending());
                        std::task::Poll::Ready(())
                    })).expect("send open");
                    scope.block_on(dope_fiber::fiber!('_ => async move {
                        while Once::new(|access| amqp.channels().flow_active(access, target, lease))
                            .await.expect("channel state") != Some(true)
                        {
                            Yield::new().await;
                        }
                        let error = Once::new(move |access| {
                            drop(opening);
                            amqp.methods().reserve(
                                access,
                                target,
                                lease,
                                queue::Purge { queue: short::Name::EMPTY },
                            )
                        }).await.err().expect("abandoned open must close admission before reuse");
                        assert!(matches!(error.error(), failure::Error::Closed), "{:?}", error.error());
                        while Once::new(|access| amqp.channels().current(access, 1)).await.is_some() {
                            Yield::new().await;
                        }
                        complete.notify().expect("close completion");
                    })).expect("fiber drive");
                },
                recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("broker");
        },
    ).expect("scripted broker");
}

#[test]
fn wakes_publish_after_notified_waiter_is_cancelled() {
    use std::task::Poll;

    use dope_amqp091::adapter::publish;
    use dope_amqp091::message::property::Properties;
    use dope_amqp091::protocol::{Confirmation, method::confirm};
    use dope_fiber::task::batch::{Join, Pool};

    use crate::common::amqp::wire::Frame;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream)
                .expect("channel open")
                .check(1, 20, 10)
                .expect("open route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream)
                .expect("confirm select")
                .check(1, 85, 10)
                .expect("select route");
            Method::write(stream, 1, 85, 11, &[]).expect("select reply");
            let sent = Method::read(stream).expect("surviving publish");
            sent.check(1, 60, 40).expect("publish route");
            assert_eq!(
                common::publish::routing_key(&sent).expect("routing key"),
                "second"
            );
            Frame::read(stream)
                .expect("empty publish header")
                .check(2, 1)
                .expect("header route");
            Method::write(stream, 1, 60, 80, &[0, 0, 0, 0, 0, 0, 0, 1, 0])
                .expect("surviving publish acknowledgement");
        },
        |broker| {
            with_client(
                broker.addr(),
                16,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let target = scope
                        .block_on(Ready {
                            state: amqp.connection().observe(),
                            wait: fanout::Registration::new(),
                            gate: None,
                        })
                        .expect("fiber drive")
                        .expect("connection ready");
                    let lease = scope
                        .block_on(Open {
                            adapter: amqp,
                            target,
                            lease: None,
                            call: None,
                        })
                        .expect("fiber drive")
                        .expect("channel open");
                    let mut select = scope
                        .block_on(Once::new(|access| {
                            amqp.methods()
                                .reserve(access, target, lease, confirm::Select)
                        }))
                        .expect("fiber drive")
                        .expect("select reservation");
                    scope
                        .block_on(poll_fn(|mut cx| {
                            let wake = cx.as_ref().completion();
                            let access = local::Access::new(cx.as_mut().region_token());
                            select.poll(access, wake)
                        }))
                        .expect("fiber drive")
                        .expect("confirm select");
                    let guard = scope
                        .block_on(Once::new(|access| {
                            amqp.methods().reserve(
                                access,
                                target,
                                lease,
                                queue::Purge {
                                    queue: short::Name::EMPTY,
                                },
                            )
                        }))
                        .expect("fiber drive")
                        .expect("held method");
                    let properties = Properties::default();
                    let properties = &properties;
                    let fibers = [(true, None), (false, Some(guard))].map(|(cancel, mut guard)| {
                        let mut send = None;
                        let mut waiting = false;
                        poll_fn(move |mut cx| {
                            if cancel && waiting {
                                drop(send.take());
                                return Poll::Ready(Ok(()));
                            }
                            let wake = cx.as_ref().completion();
                            let access = local::Access::new(cx.as_mut().region_token());
                            let send = send.get_or_insert_with(|| {
                                amqp.publish()
                                    .confirm(
                                        access.read(),
                                        target,
                                        lease,
                                        publish::Request {
                                            exchange: "",
                                            routing_key: if cancel { "first" } else { "second" },
                                            mandatory: false,
                                            properties,
                                            body: &[],
                                        },
                                    )
                                    .expect("publish request")
                            });
                            let result = send.poll(access, wake);
                            if !waiting {
                                assert!(result.is_pending(), "held method must block each publish");
                                waiting = true;
                                if let Some(guard) = guard.take() {
                                    // B follows A in Join's initial polling order.
                                    drop(guard);
                                }
                            }
                            result.map(|result| {
                                result.map(|reply| assert_eq!(reply, Confirmation::Ack))
                            })
                        })
                    });
                    let mut pool = scope
                        .block_on(Pool::<2>::acquire())
                        .expect("fiber drive")
                        .expect("batch pool");
                    let results = scope
                        .block_on(Join::from_array(&mut pool, fibers))
                        .expect("surviving publish must retain its wake")
                        .expect("publish batch");
                    for result in results {
                        result.expect("publish waiter completion");
                    }
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("publish cancellation broker");
        },
    )
    .expect("scripted broker");
}

#[test]
fn keeps_publish_credit_while_close_waits_for_input() {
    use dope::core::driver::schedule::ready::wake::queue::Registration;
    use dope_amqp091::adapter::publish;
    use dope_amqp091::message::property::Properties;
    use dope_fiber::task::batch::{Join, Pool};

    use crate::common::amqp::wire::Frame;
    use crate::common::wake::Flow;

    TcpScript::with(
        |stream| {
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("broker read deadline");
            Peer::new(stream).open(TUNE).expect("handshake");
            let channel = common::basic::returned::open_channel(stream).expect("channel");
            Method::read(stream)
                .expect("barrier")
                .check(channel, 50, 30)
                .expect("purge route");
            let heartbeat = Frame::try_encode(8, 0, &[]).expect("heartbeat");
            let mut prefix =
                Method::try_encode(channel, 50, 31, &0u32.to_be_bytes()).expect("barrier reply");
            prefix.extend_from_slice(&heartbeat[..2]);
            stream
                .write_all(&prefix)
                .expect("barrier and partial heartbeat");
            Method::read(stream)
                .expect("sole credit owner")
                .check(channel, 20, 20)
                .expect("flow route");
            common::client::check_publish(stream, channel, "survivor", &[])
                .expect("plain publish must retain the released credit wake");
            stream.write_all(&heartbeat[2..]).expect("heartbeat suffix");
            common::basic::returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().egress(config::Egress::new(1, 4096)));
            Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _>(
                Policy::new(1, 16, Duration::from_secs(10)),
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    scope.block_on(dope_fiber::fiber!('_ => async move {
                        let target = Ready {
                            state: amqp.connection().observe(),
                            wait: fanout::Registration::new(),
                            gate: None,
                        }.await.expect("connection ready");
                        let lease = Open {
                            adapter: amqp,
                            target,
                            lease: None,
                            call: None,
                        }.await.expect("channel open");
                        let mut barrier = Once::new(|access| {
                            amqp.methods().reserve(
                                access,
                                target,
                                lease,
                                queue::Purge { queue: short::Name::EMPTY },
                            )
                        }).await.expect("barrier reservation");
                        Purge { call: &mut barrier, gate: None }.await.expect("barrier reply");
                        let mut pool = Pool::<2>::acquire().await.expect("batch pool");
                        let properties = Properties::default();
                        let properties = &properties;
                        let fibers = [true, false].map(|closing| {
                            let mut close = closing.then(|| {
                                amqp.connection().control().close(target, 200, "done")
                            });
                            let mut send = None;
                            let mut first = true;
                            poll_fn(move |mut cx| {
                                let wake = cx.as_ref().completion();
                                let access = local::Access::new(cx.as_mut().region_token());
                                let result = if let Some(close) = &mut close {
                                    close.poll(access, wake)
                                } else {
                                    let send = send.get_or_insert_with(|| {
                                        amqp.publish().send(
                                            access.read(),
                                            target,
                                            lease,
                                            publish::Request {
                                                exchange: "",
                                                routing_key: "survivor",
                                                mandatory: false,
                                                properties,
                                                body: &[],
                                            },
                                        ).expect("publish request")
                                    });
                                    send.poll(access, wake)
                                };
                                if first {
                                    assert!(result.is_pending(), "input or sole credit must block");
                                    first = false;
                                }
                                result
                            })
                        });
                        Flow {
                            adapter: amqp,
                            target: target.epoch(),
                            lease,
                            credit: Registration::new(),
                            gate: None,
                        }.await.expect("fill the sole regular frame");
                        // Join registers close before publish, without an intervening I/O turn.
                        let results = Join::from_array(&mut pool, fibers).await.expect("batch");
                        for result in results {
                            result.expect("close and independent publish must complete");
                        }
                    })).expect("fiber drive");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("broker");
        },
    )
    .expect("scripted broker");
}

#[test]
fn consumes_once() {
    TcpScript::with(
        |stream| -> Result<(), crate::common::amqp::Error> {
            assert!(
                Peer::new(stream).open(TUNE).is_ok(),
                "AMQP broker handshake"
            );
            Method::read(stream)?
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream.write_all(
                &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
            )?;
            for count in [7_u32, 11] {
                Method::read(stream)?
                    .check(1, 50, 30)
                    .expect("AMQP method route");
                stream.write_all(
                    &Method::try_encode(1, 50, 31, &count.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )?;
            }
            Ok(())
        },
        |broker| {
            let config = (config::Config::new("guest", "guest"))
                .expect("method configuration must be valid");
            let config = config.capacity(
                config::Capacity::default().channel(config::Channel::default().methods(1)),
            );
            let policy = Policy::new(1, 16, Duration::from_secs(10));
            {
                let mut reports = Vec::new();
                Endpoint::new(broker.addr(), config).enter::<timing::Throughput, _, _>(
                    policy,
                    |scope| {
                        let amqp = adapter::Adapter::new(scope.port());
                        let target = scope
                            .block_on(Ready {
                                state: amqp.connection().observe(),
                                wait: fanout::Registration::new(),
                                gate: None,
                            })
                            .expect("fiber drive")
                            .expect("method connection must become ready");
                        let lease = scope
                            .block_on(Open {
                                adapter: amqp,
                                target,
                                lease: None,
                                call: None,
                            })
                            .expect("fiber drive")
                            .expect("method channel must open");
                        let mut first = scope
                            .block_on(Once::new(move |local| {
                                amqp.methods().reserve(
                                    local,
                                    target,
                                    lease,
                                    queue::Purge {
                                        queue: short::Name::EMPTY,
                                    },
                                )
                            }))
                            .expect("fiber drive")
                            .expect("first method must reserve the sole slot");
                        let reply = scope
                            .block_on(Purge {
                                call: &mut first,
                                gate: None,
                            })
                            .expect("fiber drive")
                            .expect("first method must complete");
                        assert_eq!(reply.message_count, 7);
                        let mut next = scope
                            .block_on(Once::new(move |local| {
                                amqp.methods().reserve(
                                    local,
                                    target,
                                    lease,
                                    queue::Purge {
                                        queue: short::Name::EMPTY,
                                    },
                                )
                            }))
                            .expect("fiber drive")
                            .expect("completion must release the sole slot");
                        assert!(matches!(
                            scope.block_on(Purge {
                                call: &mut first,
                                gate: None
                            }),
                            Ok(Err(Fault::Runtime(failure::Runtime::Local(
                                failure::Error::Polled
                            ))))
                        ));
                        drop(first);
                        let reply = scope
                            .block_on(Purge {
                                call: &mut next,
                                gate: None,
                            })
                            .expect("fiber drive")
                            .expect("the reused slot must retain its own response");
                        assert_eq!(reply.message_count, 11);
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
            assert!(matches!(broker.finish(), Ok(Ok(()))));
        },
    )
    .expect("method broker must start");
}

#[test]
fn drops_unbound_consumer() {
    let (observed, notify) = wake::Ends::blocking().expect("cancel observations").split();
    const TAGS: [&str; 2] = ["unbound", "reused"];

    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");
            for tag in TAGS {
                check_client_consume(&Method::read(stream).expect("basic.consume"), tag)
                    .expect("AMQP consumer method");
                let reply = consume_ok_arguments(tag).expect("AMQP consumer tag");
                stream
                    .write_all(&Method::try_encode(1, 60, 21, &reply).expect("AMQP frame fixture"))
                    .expect("basic.consume-ok");
                check(&Method::read(stream).expect("basic.cancel"), tag)
                    .expect("AMQP cancel method");
                stream
                    .write_all(&Method::try_encode(1, 60, 31, &reply).expect("AMQP frame fixture"))
                    .expect("basic.cancel-ok");
                notify.notify().expect("cancel acknowledged");
            }
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(
                    config::Capacity::default()
                        .channel(config::Channel::default().methods(1).consumers(1)),
                );
            Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _>(
                Policy::new(1, 16, Duration::from_secs(10)),
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let target = scope
                        .block_on(Ready {
                            state: amqp.connection().observe(),
                            wait: fanout::Registration::new(),
                            gate: None,
                        })
                        .expect("fiber drive")
                        .expect("connection ready");
                    let lease = scope
                        .block_on(Open {
                            adapter: amqp,
                            target,
                            lease: None,
                            call: None,
                        })
                        .expect("fiber drive")
                        .expect("channel open");
                    let table = Table::new();
                    for tag in TAGS {
                        let request = basic::Consume {
                            queue: short::Short::resource("q", short::Resource::Queue)
                                .expect("queue name"),
                            consumer_tag: short::Short::new(tag).expect("consumer tag"),
                            no_local: false,
                            ack: ack::Manual,
                            exclusive: false,
                            arguments: &table,
                        };
                        let mut call = scope
                            .block_on(Once::new(move |access| {
                                amqp.methods().reserve(access, target, lease, request)
                            }))
                            .expect("fiber drive")
                            .expect("method reservation");
                        let value = scope
                            .block_on(poll_fn(|mut cx| {
                                let wake = cx.as_ref().completion();
                                let access = local::Access::new(cx.as_mut().region_token());
                                call.poll(access, wake)
                            }))
                            .expect("fiber drive")
                            .expect("consumer registration must reuse the sole slot");
                        let (_, counts) = Tracker::usage(|| drop(value));
                        assert_eq!(counts, (0, 0));
                        scope
                            .block_on(wait(&observed))
                            .expect("fiber drive")
                            .expect("cancel acknowledgement");
                    }
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker
                .finish()
                .expect("consumer cancellation broker completion");
        },
    )
    .expect("consumer cancellation broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
