use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::platform::wake;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once, wait};
pub mod common;

use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn orphan_preempts_owned() {
    let (seen, notify) = wake::Ends::blocking().expect("request observation").split();
    let (dropped, release) = wake::Ends::blocking().expect("drop barrier").split();
    let (retired, finish) = wake::Ends::blocking()
        .expect("retirement observation")
        .split();
    const OWNED: &str = "owned-cancel";
    const ORPHAN: &str = "orphan-cancel";

    TcpScript::with(
        move |stream| {
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

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let owned_ok = consume_ok_arguments(OWNED).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &owned_ok).expect("AMQP frame fixture"))
                .expect("owned consume-ok");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            notify.notify().expect("orphan request");
            dropped
                .wait_timeout(Duration::from_secs(10))
                .expect("consumer drop");
            let orphan = consume_ok_arguments(ORPHAN).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &orphan).expect("AMQP frame fixture"))
                .expect("orphan consume-ok");

            check(&Method::read(stream).expect("AMQP method"), ORPHAN).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &orphan).expect("AMQP frame fixture"))
                .expect("orphan cancel-ok");
            check(&Method::read(stream).expect("AMQP method"), OWNED).expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &owned_ok).expect("AMQP frame fixture"))
                .expect("owned cancel-ok");
            finish.notify().expect("consumers retired");
        },
        |broker| {
            let addr = broker.addr();

            let channel = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(2)
                .deliveries(1)
                .returns(1);
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(channel)
                .egress(config::Egress::new(2, 4096));
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(capacity);

            with_client(
                addr,
                config,
                |runtime| {
                    let conn = runtime.connection();
                    assert!(
                        matches!(runtime.block_on(conn.observe().ready()), Ok(Ok(_))),
                        "AMQP operation must succeed"
                    );
                    let channel = ((runtime.block_on(conn.channels().open(1)))
                        .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let args = Table::new();
                    let owned = ((runtime.block_on(channel.basic().consume(basic::Consume {
                        queue: "q",
                        consumer_tag: OWNED,
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: false,
                        arguments: &args,
                    })))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let mut orphan = Pinned::new(channel.basic().consume(basic::Consume {
                        queue: "q",
                        consumer_tag: ORPHAN,
                        no_local: false,
                        ack: ack::Manual,
                        exclusive: false,
                        arguments: &args,
                    }));
                    assert!(
                        matches!(
                            runtime.block_on(poll_once(orphan.as_pin())),
                            Ok(std::task::Poll::Pending)
                        ),
                        "AMQP fiber must complete"
                    );
                    runtime
                        .block_on(wait(&seen))
                        .expect("fiber drive")
                        .expect("orphan request wait");
                    let (_, alloc) = Tracker::usage(|| {
                        drop(owned);
                        drop(orphan);
                    });
                    assert_eq!(alloc, (0, 0));
                    release.notify().expect("consumers dropped");
                    runtime
                        .block_on(wait(&retired))
                        .expect("fiber drive")
                        .expect("retirement wait");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("broker completion");
        },
    )
    .expect("broker");
}

#[test]
fn resumes_cancellation_and_control_after_shared_quota() {
    use std::pin::Pin;
    use std::task::Poll;

    use dope::core::driver::life::creation::Attempt;
    use dope::core::driver::schedule::ready::wake::fanout;
    use dope::core::driver::settings::claim::Needs;
    use dope::manifold::app::{Host, env::Bundle};
    use dope::manifold::service::{self, health, sync};
    use dope::manifold::timing;
    use dope::net::tcp::Tcp;
    use dope::net::wire::{Identity, domain};
    use dope::runtime::executor::{self, session};
    use dope_amqp091::adapter::{self, Poll as _};
    use dope_amqp091::protocol::method::{basic, queue};
    use dope_amqp091::protocol::wire::text::short;
    use dope_amqp091::service::{Port, Spec, app};
    use dope_amqp091_fiber::conn::Connection;
    use dope_fiber::abi::poll_fn;
    use dope_fiber::task::local::Once;
    use dope_fiber::task::time::sleep::TimerExt as _;
    use dope_test::fibers::TEST;
    use dope_test::fibers::Yield;
    use o3::base::cell::local;

    const TAG: &str = "shared-quota";
    const CANCELLED: usize = dope::core::driver::schedule::LIMIT;
    for control in [false, true] {
        let (observed, notify) = wake::Ends::blocking()
            .expect("cancellation observation")
            .split();
        let (release, suffix) = wake::Ends::blocking().expect("input barrier").split();
        TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("channel.open route");
            stream
                .write_all(&Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("open fixture"))
                .expect("channel.open-ok");
            check_client_consume(&Method::read(stream).expect("basic.consume"), TAG)
                .expect("consumer route");
            let reply = consume_ok_arguments(TAG).expect("consumer tag");
            let mut response = Method::try_encode(1, 60, 21, &reply).expect("consume fixture");
            response.extend_from_slice(
                &Method::try_encode(0, 10, 60, b"\x05ready").expect("barrier fixture"),
            );
            let heartbeat = crate::common::amqp::wire::Frame::try_encode(8, 0, &[])
                .expect("heartbeat fixture");
            if control {
                response.extend_from_slice(&heartbeat[..2]);
            }
            stream.write_all(&response).expect("consume response barrier");
            if control {
                release.wait_timeout(Duration::from_secs(10)).expect("input release");
                stream.write_all(&heartbeat[2..]).expect("heartbeat suffix");
            }
            check(&Method::read(stream).expect("basic.cancel after shared quota"), TAG)
                .expect("consumer cancellation");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &reply).expect("cancel fixture"))
                .expect("basic.cancel-ok");
            notify.notify().expect("consumer cancelled");
            stream
                .shutdown(std::net::Shutdown::Both)
                .expect("cancellation observation wake");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().channels(1).channel(
                    config::Channel::default()
                        .methods(CANCELLED + 1)
                        .consumers(1),
                ));
            let spec = Spec::<0>::try_new(config, 1, CANCELLED).expect("AMQP plan");
            let plan = app::Plan::try_for(
                &spec,
                Duration::from_millis(500),
                Duration::from_secs(30),
            )
            .expect("connector plan");
            let inputs = (spec, plan);
            let config = Needs::from_tree(&inputs)
                .and_then(|needs| needs.resolve::<timing::Throughput>())
                .expect("driver plan");
            let (spec, plan) = inputs;
            let mut attempt = Attempt::new();
            let executor = executor::Executor::new(&mut attempt, config)
                .unwrap_or_else(|_| panic!("driver construction"));
            let mut outcome = session::Completion::new();
            executor.run(|active| {
                let result = active.enter_with(
                    |mut runtime| {
                        Port::try_with(spec, &mut runtime, session::Scope::driver, |port, runtime| {
                            let snapshot = service::Snapshot::from_one(
                                service::Revision::new(1),
                                service::Endpoint::new(broker.addr(), broker.addr()),
                            );
                            let backoff = runtime.hash_state(health::Domain::DEFAULT);
                            plan.into_inner()
                                .bind(domain::Local::new(Identity), backoff)
                                .install::<_, _, sync::Preserve, _, Bundle<Tcp, Identity, timing::Throughput>, 1, _, _>(
                                    port.bind(()),
                                    snapshot,
                                    runtime,
                                    session::Scope::driver,
                                    |host, runtime| {
                                        runtime.with_app(
                                            &mut outcome,
                                            Split::new(Host::new(host)),
                                            |mut app| {
                                                let conn = Connection::new(&mut app, |root| {
                                                    Pin::new(root.primary().manifold().get_ref().session())
                                                });
                                                let amqp = adapter::Adapter::new(&port);
                                                let target = TEST.try_run(&mut app, conn.observe().ready())
                                                    .expect("ready drive")
                                                    .expect("connection ready");
                                                let channel = TEST.try_run(&mut app, conn.channels().open(1))
                                                    .expect("open drive")
                                                    .expect("channel open");
                                                let table = Table::new();
                                                let mut consume = TEST.try_run(&mut app, Once::new(|mut access| {
                                                    let lease = amqp.channels().current(access.reborrow(), 1)
                                                        .expect("open lease");
                                                    amqp.methods().reserve(access, target, lease, basic::Consume {
                                                        queue: short::Name::EMPTY,
                                                        consumer_tag: short::Short::new(TAG).expect("consumer tag"),
                                                        no_local: false,
                                                        ack: ack::Manual,
                                                        exclusive: false,
                                                        arguments: &table,
                                                    })
                                                }))
                                                .expect("consume reservation drive")
                                                .expect("consume reservation");
                                                TEST.try_run(&mut app, poll_fn(|mut cx| {
                                                    let wake = cx.as_ref().completion();
                                                    let access = local::Access::new(cx.as_mut().region_token());
                                                    assert!(consume.poll(access, wake).is_pending());
                                                    Poll::Ready(())
                                                }))
                                                .expect("consume send");
                                                let timer = port.timer();
                                                TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                                                    while !conn.observe().read(|view| view.is_blocked()).await {
                                                        timer.sleep(Duration::from_millis(1))
                                                            .expect("response barrier timer").await;
                                                    }
                                                }))
                                                .expect("completed consume barrier");
                                                let mut subscriptions = [const { fanout::Registration::new() }; CANCELLED];
                                                TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                                                    Once::new(|mut access| {
                                                        let lease = amqp.channels().current(access.reborrow(), 1)
                                                            .expect("open lease");
                                                        let mut calls = Vec::with_capacity(CANCELLED);
                                                        for _ in 0..CANCELLED {
                                                            calls.push(amqp.methods().reserve(
                                                                access.reborrow(),
                                                                target,
                                                                lease,
                                                                queue::Purge { queue: short::Name::EMPTY },
                                                            ).expect("unsent method reservation"));
                                                        }
                                                        drop(calls);
                                                        drop(consume);
                                                    }).await;
                                                    if control {
                                                        // Drain the cancellation queue while the partial frame
                                                        // holds its resulting control write behind input.
                                                        for _ in 0..=CANCELLED {
                                                            Yield::new().await;
                                                        }
                                                    }
                                                }))
                                                .expect("queue cancellation");
                                                if control {
                                                    TEST.try_run(&mut app, poll_fn(|cx| {
                                                        let wake = cx.as_ref().completion();
                                                        for subscription in &mut subscriptions {
                                                            amqp.channels().watch_flow(subscription, &wake)
                                                                .expect("flow subscription");
                                                        }
                                                        Poll::Ready(())
                                                    })).expect("retired wake targets");
                                                    suffix.notify().expect("release held control write");
                                                }
                                                TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                                                    conn.observe().retired(target.epoch()).await
                                                        .expect("broker retirement wake");
                                                    observed.wait_timeout(Duration::ZERO)
                                                        .expect("consumer cancellation observation");
                                                }))
                                                .unwrap_or_else(|error| panic!("shared quota control={control}: {error:?}"));
                                                drop(channel);
                                            },
                                        ).unwrap_or_else(|_| panic!("application lifecycle"));
                                    },
                                )
                                .unwrap_or_else(|_| panic!("connector installation"));
                        })
                        .expect("AMQP storage");
                    },
                    recovery::Policy::new(|_| {}, recover),
                );
                assert!(result.is_ok(), "native lifecycle");
            });
            broker.finish().expect("broker completion");
        },
    )
    .expect("broker");
    }
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
