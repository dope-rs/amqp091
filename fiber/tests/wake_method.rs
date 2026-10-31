use std::convert::Infallible;
use std::io;
use std::io::Write as _;

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
fn consumes_once() {
    TcpScript::with(
        |stream| -> io::Result<()> {
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
