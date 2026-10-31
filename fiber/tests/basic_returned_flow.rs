use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::Frame;
pub mod common;

use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dope::core::io::Reclaimed;
use dope::manifold::timing;
use dope_amqp091::message::property::Properties;
use dope_amqp091::{adapter, config};
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::fibers::Yield;
use dope_test::harness::peer::TcpScript;

use crate::common::basic;
use crate::common::basic::returned;

#[test]
fn interleaves_by_channel() {
    const FIRST_BODY: &[u8] = b"left";

    TcpScript::with(
        |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            let first = returned::open_channel(stream).expect("AMQP fixture");
            let second = returned::open_channel(stream).expect("AMQP fixture");
            assert_ne!(first, second);

            returned::check_publish(
                stream,
                first,
                basic::EXCHANGE,
                "official.first",
                FIRST_BODY,
                None,
            )
            .expect("AMQP fixture");
            returned::check_publish(stream, second, basic::EXCHANGE, "official.empty", &[], None)
                .expect("AMQP fixture");

            stream
                .write_all(
                    &returned::method(first, basic::EXCHANGE, "official.first")
                        .expect("AMQP fixture"),
                )
                .expect("first basic.return");
            stream
                .write_all(
                    &basic::content_header(first, FIRST_BODY.len() as u64).expect("AMQP fixture"),
                )
                .expect("first content header");
            stream
                .write_all(&Frame::try_encode(3, first, b"le").expect("AMQP frame fixture"))
                .expect("first partial body");
            stream
                .write_all(
                    &returned::content(second, basic::EXCHANGE, "official.empty", &[], None)
                        .expect("AMQP fixture"),
                )
                .expect("second zero-body return");
            stream
                .write_all(&Frame::try_encode(3, first, b"ft").expect("AMQP frame fixture"))
                .expect("first final body");
            returned::close_connection(stream).expect("AMQP fixture");
        },
        |broker| {
            let chan = config::Channel::default().methods(1).returns(1);
            let cap = config::Capacity::default()
                .channels(2)
                .channel(chan)
                .egress(config::Egress::new(4, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(cap);

            {
                let mut reports = Vec::new();
                Endpoint::new(broker.addr(), cfg).enter::<timing::Throughput, _, _>(
                    basic::POLICY,
                    |rt| {
                        let conn = rt.connection();
                        assert!(
                            rt.block_on(dope_fiber::fiber!('_ => async move {
                                let ready = conn.observe().ready().await.expect("ready");
                                let first = conn.channels().open(1).await.expect("first channel");
                                let second = conn.channels().open(2).await.expect("second channel");
                                let props = Properties::default();
                                first
                                    .basic()
                                    .publish(adapter::publish::Request {
                                        exchange: basic::EXCHANGE,
                                        routing_key: "official.first",
                                        mandatory: true,
                                        properties: &props,
                                        body: FIRST_BODY,
                                    })
                                    .await
                                    .expect("first mandatory publish");
                                second
                                    .basic()
                                    .publish(adapter::publish::Request {
                                        exchange: basic::EXCHANGE,
                                        routing_key: "official.empty",
                                        mandatory: true,
                                        properties: &props,
                                        body: &[],
                                    })
                                    .await
                                    .expect("second mandatory publish");

                                let empty = second
                                    .next_return()
                                    .await
                                    .expect("second return state")
                                    .expect("second basic.return");
                                assert_eq!(empty.routing_key(), "official.empty");
                                assert!(empty.body().is_empty());
                                assert_eq!(empty.body().as_slice(), Some(&[][..]));

                                let fragmented = first
                                    .next_return()
                                    .await
                                    .expect("first return state")
                                    .expect("first basic.return");
                                assert_eq!(fragmented.routing_key(), "official.first");
                                assert!(fragmented.body() == FIRST_BODY[..]);
                                empty.release();
                                fragmented.release();
                                conn
                                    .control()
                                    .close(ready, 200, "official interleave complete")
                                    .await
                                    .expect("connection close");
                            }))
                            .is_ok(),
                            "AMQP fiber must complete"
                        );
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
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

#[test]
fn held_returns_block() {
    let sent = AtomicBool::new(false);
    use dope_fiber::abi::race::{Either, Race};

    let sent = &sent;
    let peer = sent;
    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            let channel = returned::open_channel(stream).expect("AMQP fixture");
            for (route, body) in [
                ("official.return.a", b"a".as_slice()),
                ("official.return.b", b"b".as_slice()),
                ("official.return.c", b"c".as_slice()),
            ] {
                returned::check_publish(stream, channel, basic::EXCHANGE, route, body, None)
                    .expect("AMQP fixture");
            }
            for (route, body) in [
                ("official.return.a", b"a".as_slice()),
                ("official.return.b", b"b".as_slice()),
                ("official.return.c", b"c".as_slice()),
            ] {
                stream
                    .write_all(
                        &returned::content(channel, basic::EXCHANGE, route, &[body], None)
                            .expect("AMQP fixture"),
                    )
                    .expect("basic.return content");
            }
            peer.store(true, Ordering::Release);
            returned::close_connection(stream).expect("AMQP fixture");
        },
        |broker| {
            let chan = config::Channel::default().methods(1).returns(2);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .body(config::Body::default().buffer(2))
                .egress(config::Egress::new(4, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(cap);

            {
                let mut reports = Vec::new();
                Endpoint::new(broker.addr(), cfg).enter::<timing::Throughput, _, _>(
                    basic::POLICY,
                    |rt| {
                        let conn = rt.connection();
                        let timer = rt.timer();
                        assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel open");
                    let props = Properties::default();
                    for (route, body) in [
                        ("official.return.a", b"a".as_slice()),
                        ("official.return.b", b"b".as_slice()),
                        ("official.return.c", b"c".as_slice()),
                    ] {
                        channel
                            .basic()
                            .publish(adapter::publish::Request {
                                exchange: basic::EXCHANGE,
                                routing_key: route,
                                mandatory: true,
                                properties: &props,
                                body,
                            })
                            .await
                            .expect("mandatory publish");
                    }
                    while !sent.load(Ordering::Acquire) {
                        let sleep = (timer.sleep(Duration::from_millis(1))).expect("broker send wait");
                        sleep.await;
                    }

                    let first = channel
                        .next_return()
                        .await
                        .expect("first return state")
                        .expect("first basic.return");
                    let second = channel
                        .next_return()
                        .await
                        .expect("second return state")
                        .expect("second basic.return");
                    assert_eq!(first.routing_key(), "official.return.a");
                    assert!(first.body() == b"a"[..]);
                    assert_eq!(second.routing_key(), "official.return.b");
                    assert!(second.body() == b"b"[..]);

                    let result = Race::new(channel.next_return(), Yield::new()).await;
                    assert!(matches!(result, Either::Right(())));

                    first.release();
                    let third = channel
                        .next_return()
                        .await
                        .expect("third return state")
                        .expect("third basic.return");
                    assert_eq!(third.routing_key(), "official.return.c");
                    assert!(third.body() == b"c"[..]);
                    assert!(second.body() == b"b"[..]);
                    second.release();
                    third.release();
                    conn
                        .control()
                        .close(ready, 200, "official return backpressure complete")
                        .await
                        .expect("connection close");
                })).is_ok(), "AMQP fiber must complete");
                    },
                    |event| reports.push(event), recover,
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
            broker.finish().expect("scripted AMQP broker completion");
        },
    ).expect("scripted AMQP broker");
}

#[test]
fn return_waiter_is_exclusive() {
    use std::cell::Cell;
    use std::task::Poll;

    use dope::core::driver::schedule::ready::wake::table;
    use dope_amqp091::failure;
    use dope_fiber::abi::PollFn;
    use dope_fiber::task::batch::{Join, Pool};

    use crate::common::amqp::wire::Method;
    use crate::common::fiber::{Pinned, poll_once};

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(basic::TUNE).expect("handshake");
            let channel = returned::open_channel(stream).expect("channel");
            for body in [b"a", b"b"] {
                Method::read(stream).expect("return barrier").check(channel, 50, 30)
                    .expect("barrier method");
                Method::write(stream, channel, 50, 31, &[0; 4]).expect("barrier reply");
                stream.write_all(&returned::content(channel, "", "", &[body], None)
                    .expect("return frame")).expect("return body");
            }
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
                        let channel = conn.channels().open(1).await.expect("channel");
                        let registered = Cell::new(false);
                        let channel = &channel;
                        let registered = &registered;
                        let waiters = [false, true].map(|contender| dope_fiber::fiber!('_ => async move {
                            if contender {
                                while !registered.get() {
                                    Yield::new().await;
                                }
                                {
                                    let mut rejected = Pinned::new(channel.next_return());
                                    assert!(matches!(poll_once(rejected.as_pin()).await,
                                        Poll::Ready(Err(failure::Runtime::Local(failure::Error::Table(
                                            table::Error::Occupied { .. }
                                        ))))));
                                }
                                channel.queue().purge("first").await.expect("return barrier");
                            } else {
                                let mut first = Pinned::new(channel.next_return());
                                let first = PollFn::admitted(|admit| {
                                    let poll = admit.poll(first.as_pin()).0;
                                    if poll.is_pending() {
                                        registered.set(true);
                                    }
                                    poll
                                }).await.expect("original waiter").expect("first return");
                                assert_eq!(first.body().as_slice(), Some(b"a".as_slice()));
                                first.release();
                            }
                        }));
                        let mut pool = Pool::<2>::acquire().await.expect("waiter tasks");
                        Join::from_array(&mut pool, waiters).await.expect("original waiter wake");
                        {
                            let mut canceled = Pinned::new(channel.next_return());
                            assert!(poll_once(canceled.as_pin()).await.is_pending());
                        }
                        let mut next = Pinned::new(channel.next_return());
                        assert!(poll_once(next.as_pin()).await.is_pending());
                        channel.queue().purge("next").await.expect("next return barrier");
                        let next = next.await.expect("new waiter").expect("next return");
                        assert_eq!(next.body().as_slice(), Some(b"b".as_slice()));
                        next.release();
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
fn close_wakes_return() {
    use std::cell::Cell;

    use dope_fiber::abi::PollFn;
    use dope_fiber::task::batch::{Join, Pool};

    use crate::common::amqp::wire::Method;
    use crate::common::fiber::Pinned;

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(basic::TUNE).expect("handshake");
            let channel = returned::open_channel(stream).expect("channel");
            Method::read(stream).expect("channel close").check(channel, 20, 40)
                .expect("close method");
            Method::write(stream, channel, 20, 41, &[]).expect("channel close-ok");
            let channel = returned::open_channel(stream).expect("reopened channel");
            Method::read(stream).expect("return barrier").check(channel, 50, 30)
                .expect("barrier method");
            Method::write(stream, channel, 50, 31, &[0; 4]).expect("barrier reply");
            stream.write_all(&returned::content(channel, "", "", &[b"next"], None)
                .expect("return frame")).expect("reopened return");
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
                        let channel = conn.channels().open(1).await.expect("channel");
                        let registered = Cell::new(false);
                        let channel = &channel;
                        let registered = &registered;
                        let waiters = [false, true].map(|closing| dope_fiber::fiber!('_ => async move {
                            if closing {
                                while !registered.get() {
                                    Yield::new().await;
                                }
                                channel.close(200, "done").await.expect("channel close");
                            } else {
                                let mut returned = Pinned::new(channel.next_return());
                                let result = PollFn::admitted(|admit| {
                                    let poll = admit.poll(returned.as_pin()).0;
                                    if poll.is_pending() {
                                        registered.set(true);
                                    }
                                    poll
                                }).await;
                                let error = result.err().expect("closed return stream");
                                assert!(matches!(error.error(), dope_amqp091::failure::Error::Closed));
                            }
                        }));
                        let mut pool = Pool::<2>::acquire().await.expect("waiter tasks");
                        Join::from_array(&mut pool, waiters).await.expect("close wake");
                        let channel = conn.channels().open_auto().await.expect("reuse closed channel");
                        let mut next = Pinned::new(channel.next_return());
                        assert!(common::fiber::poll_once(next.as_pin()).await.is_pending());
                        channel.queue().purge("next").await.expect("return barrier");
                        let next = next.await.expect("new registration").expect("new return");
                        assert_eq!(next.body().as_slice(), Some(b"next".as_slice()));
                        next.release();
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
