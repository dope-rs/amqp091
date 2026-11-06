use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_test::alloc;

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Greeting, Peer};
pub mod common;

use std::task;

use common::publish::blocked::Blocked;
use dope_amqp091::failure::Error;
use dope_amqp091::message::property::Properties;
use dope_amqp091::{adapter, failure};
use dope_fiber::task::local::Once;
use dope_test::alloc::Tracker;
use dope_test::fibers::Yield;
use dope_test::harness::peer::TcpScript;

use crate::common::publish::{TUNE, with_client};

#[test]
fn keeps_reason_exact() {
    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .preface()
                .and_then(|peer| peer.start(Greeting::PLAIN))
                .and_then(|peer| peer.recv())
                .and_then(|peer| peer.tune(TUNE))
                .and_then(|peer| peer.recv())
                .and_then(|peer| peer.recv())
                .expect("connection open");
            let mut frames = Method::try_encode(0, 10, 41, &[0]).expect("open ok");
            frames.extend_from_slice(
                &Method::try_encode(0, 10, 60, b"\x08pressure").expect("blocked"),
            );
            stream.write_all(&frames).expect("open and blocked");
            assert_eq!(stream.read(&mut [0]).expect("blocked probe teardown"), 0);
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let conn = scope.connection();
                    let target = scope
                        .block_on(conn.observe().ready())
                        .expect("fiber drive")
                        .expect("AMQP operation must succeed");
                    let props = Properties::default();
                    assert!(
                    scope
                        .block_on(dope_fiber::fiber!('_ => async move {
                            while !Once::new(move |local| {
                                amqp
                                    .connection()
                                    .observe()
                                    .read(local.read(), |view| view.is_blocked())
                            })
                            .await
                            {
                                Yield::new().await;
                            }
                            let lease = Once::new(move |local| {
                                let reserve = amqp.channels().reserve(local, target, 1)?;
                                Ok::<_, failure::Runtime<'_, '_, '_, 0>>(reserve.commit())
                            })
                            .await
                            .expect("channel reservation");
                            let observed = Blocked {
                                adapter: amqp,
                                target,
                                lease,
                                properties: &props,
                            }
                            .await;
                            for observation in observed {
                                assert_eq!(observation.allocations, (0, 0));
                                let poll = observation.result.expect("valid blocked publish request");
                                let result = match poll {
                                    task::Poll::Ready(result) => Some(result),
                                    task::Poll::Pending => None,
                                }.expect("blocked publish completes immediately");
                                let error = result.expect_err("blocked publish must fail");
                                let reason = match error.error() {
                                    Error::ConnectionBlocked(reason) => Some(reason),
                                    _ => None,
                                }.expect("blocked publish keeps its exact reason");
                                assert_eq!(reason.as_ref(), "pressure");
                            }
                        }))
                        .is_ok(),
                    "AMQP fiber must complete"
                );
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("blocked publish broker");
        },
    ).expect("broker");
}

#[test]
fn separates_control_text_credit() {
    use common::{
        client,
        fiber::{Pinned, poll_once},
        publish::with_config,
    };
    use dope_amqp091::config;

    for topology in [0, 8] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
                Method::read(stream)
                    .expect("channel.open")
                    .check(1, 20, 10)
                    .expect("channel.open route");
                Method::write(stream, 0, 10, 60, b"\x08pressure").expect("connection.blocked");
                if topology == 0 {
                    return;
                }
                Method::write(stream, 1, 20, 11, &[0; 4]).expect("channel.open-ok");
                Method::read(stream)
                    .expect("unblock purge")
                    .check(1, 50, 30)
                    .expect("queue.purge route");
                Method::write(stream, 0, 10, 61, &[]).expect("connection.unblocked");
                stream
                    .write_all(
                        &client::returned_properties(1, b"abc", &[]).expect("return fixture"),
                    )
                    .expect("returned properties");
                Method::write(stream, 1, 50, 31, &[0; 4]).expect("queue.purge-ok");
                Method::read(stream)
                    .expect("full topology purge")
                    .check(1, 50, 30)
                    .expect("queue.purge route");
                Method::write(stream, 0, 10, 60, b"\x08pressure")
                    .expect("blocked with full topology");
            },
            |broker| {
                with_config(
                    broker.addr(),
                    config::Config::new("guest", "guest")
                        .expect("config")
                        .capacity(
                            config::Capacity::default()
                                .topology(config::Topology::new(topology))
                                .body(
                                    config::Body::default().buffer(client::RETURNED_PROPERTY_BYTES),
                                ),
                        ),
                    |scope| {
                        let conn = scope.connection();
                        assert!(scope.block_on(dope_fiber::fiber!('_ => async move {
                            conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await;
                            let error = if topology == 0 {
                                channel.err().expect("control text needs topology credit")
                            } else {
                                let channel = channel.expect("channel opened after blocked");
                                let properties = Properties::default();
                                let blocked = channel.basic().publish(client::channel::request(
                                    "", "blocked", &properties, b"body",
                                )).await.expect_err("blocked publish");
                                let mut returned = Pinned::new(channel.next_return());
                                assert!(poll_once(returned.as_pin()).await.is_pending());
                                channel.queue().purge("unblock").await.expect("unblock purge");
                                conn.observe().unblocked().await.expect("unblocked");
                                let returned = returned.await.expect("return operation")
                                    .expect("returned properties before error drop");
                                assert_eq!(returned.properties().content_type(), Some("abc"));
                                assert!(matches!(blocked.error(), Error::ConnectionBlocked(reason)
                                    if reason.as_ref() == "pressure"));
                                let error = channel.queue().purge("full").await
                                    .expect_err("retained reason fills topology credit");
                                returned.release();
                                drop(blocked);
                                error
                            };
                            assert!(matches!(error.error(), Error::Resident {
                                target: failure::wire::Allocation::Short,
                                error: failure::wire::Resident::Budget { required, available: 0 },
                            } if required.get() == 8));
                        })).is_ok(), "AMQP fiber must complete");
                    },
                    recover,
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("endpoint completion");
                    },
                );
                broker.finish().expect("control-text broker");
            },
        )
        .expect("broker");
    }
}

#[test]
fn wakes_flow_on_close_admission() {
    use dope_fiber::task::batch::{Join, Pool};

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream)
                .expect("channel.open")
                .check(1, 20, 10)
                .expect("open route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("channel.open-ok");
            Method::read(stream)
                .expect("flow fence")
                .check(1, 50, 30)
                .expect("purge route");
            Method::write(stream, 0, 10, 60, b"\x08pressure").expect("connection.blocked");
            Method::write(stream, 1, 20, 20, &[0]).expect("channel.flow");
            Method::write(stream, 1, 50, 31, &[0; 4]).expect("purge-ok after flow");
            Method::read(stream)
                .expect("channel.flow-ok")
                .check(1, 20, 21)
                .expect("flow reply route");
            Method::read(stream)
                .expect("connection.close")
                .check(0, 10, 50)
                .expect("close route");
            assert_eq!(stream.read(&mut [0]).expect("teardown without close-ok"), 0);
        },
        |broker| {
            with_client(
                broker.addr(),
                |scope| {
                    let conn = scope.connection();
                    scope
                        .block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            channel.queue().purge("fence").await.expect("flow fence");
                            let channel = &channel;
                            let fibers = std::array::from_fn::<_, 3, _>(|index| {
                                dope_fiber::fiber!('_ => async move {
                                    match index {
                                        0 => conn.observe().unblocked().await.expect("unblocked wake"),
                                        1 => {
                                            let error = channel.flow().await.expect_err("closed flow");
                                            assert!(matches!(error.error(), Error::Closed));
                                        }
                                        _ => conn.control().start(ready, 200, "done").await
                                            .expect("close admission"),
                                    }
                                })
                            });
                            let mut pool = Pool::<3>::acquire().await.expect("waiter pool");
                            // Ordered first polls register both waiters before close admission.
                            Join::from_array(&mut pool, fibers).await.expect("flow wakes");
                        }))
                        .expect("flow wake deadline");
                },
                recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("broker completion");
        },
    )
    .expect("broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();
fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
