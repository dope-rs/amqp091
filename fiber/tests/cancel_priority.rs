use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn orphan_preempts_owned() {
    const OWNED: &str = "owned-cancel";
    const ORPHAN: &str = "orphan-cancel";

    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(Tune::new(2, 4096, 0))
                .expect("AMQP broker handshake");
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
            Method::read(stream)
                .expect("orphan observation channel")
                .check(2, 20, 10)
                .expect("orphan observation route");
            Method::write(stream, 2, 20, 11, &[0, 0, 0, 0]).expect("orphan observation reply");
            Method::read(stream)
                .expect("consumer drop close")
                .check(2, 20, 40)
                .expect("consumer drop route");
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
            Method::write(stream, 2, 20, 41, &[]).expect("consumers retired reply");
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
                .channels(2)
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
                    let barrier = runtime
                        .block_on(conn.channels().open(2))
                        .expect("fiber drive")
                        .expect("orphan request observed");
                    let (_, alloc) = Tracker::usage(|| {
                        drop(owned);
                        drop(orphan);
                    });
                    assert_eq!(alloc, (0, 0));
                    runtime
                        .block_on(barrier.close(200, ""))
                        .expect("fiber drive")
                        .expect("consumers retired");
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
    use dope_fiber::task::{done, local::Once};
    use dope_test::fibers::TEST;
    use dope_test::fibers::Yield;
    use dope_test::harness::GUARD;
    use o3::base::cell::local;

    const TAG: &str = "shared-quota";
    const CANCELLED: usize = dope::core::driver::schedule::LIMIT;
    for control in [false, true] {
        TcpScript::with(
        move |stream| {
            Peer::new(stream).open(Tune::new(2, 4096, 0)).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("channel.open route");
            stream
                .write_all(&Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("open fixture"))
                .expect("channel.open-ok");
            check_client_consume(&Method::read(stream).expect("basic.consume"), TAG)
                .expect("consumer route");
            Method::read(stream)
                .expect("consume observation channel")
                .check(2, 20, 10)
                .expect("consume observation route");
            let reply = consume_ok_arguments(TAG).expect("consumer tag");
            let mut response = Method::try_encode(1, 60, 21, &reply).expect("consume fixture");
            response.extend_from_slice(
                &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("observation fixture"),
            );
            let heartbeat = crate::common::amqp::wire::Frame::try_encode(8, 0, &[])
                .expect("heartbeat fixture");
            if control {
                response.extend_from_slice(&heartbeat[..2]);
            }
            stream.write_all(&response).expect("consume response barrier");
            if control {
                crate::common::client::check_publish(stream, 2, "suffix", &[])
                    .expect("input release publish");
                stream.write_all(&heartbeat[2..]).expect("heartbeat suffix");
            }
            check(&Method::read(stream).expect("basic.cancel after shared quota"), TAG)
                .expect("consumer cancellation");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &reply).expect("cancel fixture"))
                .expect("basic.cancel-ok");
            Method::write(stream, 0, 10, 50, &[0, 200, 0, 0, 0, 0, 0])
                .expect("retirement after cancel-ok");
            Method::read(stream)
                .expect("connection.close-ok")
                .check(0, 10, 51)
                .expect("consumer cancellation processed");
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().channels(2).channel(
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
                                                TEST.try_run(&mut app, conn.channels().open(1))
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
                                                let barrier = TEST.try_run(&mut app, conn.channels().open(2))
                                                    .expect("consume observation drive")
                                                    .expect("completed consume observation");
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
                                                    let properties = dope_amqp091::message::property::Properties::default();
                                                    TEST.try_run(&mut app, barrier.basic().publish(adapter::publish::Request {
                                                        exchange: "",
                                                        routing_key: "suffix",
                                                        mandatory: false,
                                                        properties: &properties,
                                                        body: &[],
                                                    }))
                                                    .expect("input release drive")
                                                    .expect("input release publish");
                                                }
                                                app.drive_for(GUARD, done::Once::new(dope_fiber::fiber!('_ => async move {
                                                    conn.observe().retired(target.epoch()).await
                                                        .expect("broker retirement wake");
                                                })))
                                                .unwrap_or_else(|error| panic!("shared quota control={control}: {error:?}"));
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
