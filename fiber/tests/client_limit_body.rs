use std::array;
use std::convert::Infallible;
use std::io::{ErrorKind, Read as _, Write as _};

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::field::view;
use o3::buffer::{hold, resident};

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Frame, Method};
use crate::common::basic::{self, returned};
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use dope_amqp091::failure;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn rejects_content_limits() {
    for props in [false, true] {
        let body = if props {
            config::Body::default().buffer(RETURNED_PROPERTY_BYTES - 1)
        } else {
            config::Body::default().max_size(8)
        };
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
                Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                    )
                    .expect("channel open ok");
                let ready = Method::read(stream).expect("AMQP method");
                ready.check(1, 50, 30).expect("AMQP method route");
                assert_eq!(queue_name(&ready).expect("AMQP fixture"), "ready");
                stream
                    .write_all(
                        &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                            .expect("AMQP frame fixture"),
                    )
                    .expect("ready purge ok");
                let trigger = Method::read(stream).expect("AMQP method");
                trigger.check(1, 50, 30).expect("AMQP method route");
                assert_eq!(queue_name(&trigger).expect("AMQP fixture"), "trigger");

                let frames = if props {
                    returned_properties(1, b"abc", &[]).expect("AMQP fixture")
                } else {
                    let mut frames = returned::method(1, "", "").expect("AMQP fixture");
                    frames.extend_from_slice(&basic::content_header(1, 9).expect("AMQP fixture"));
                    frames
                };
                stream.write_all(&frames).expect("oversized return");
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest")
                        .expect("config")
                        .capacity(config::Capacity::default().body(body)),
                    |rt| {
                        let conn = rt.connection();
                        assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                            wait_ready(conn).await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let mut returned = Pinned::new(channel.next_return());
                            assert!(poll_once(returned.as_pin()).await.is_pending());
                            channel.queue().purge("ready").await.expect("ready purge");
                            let mut trigger = Pinned::new(channel.queue().purge("trigger"));
                            assert!(poll_once(trigger.as_pin()).await.is_pending());
                            let error = returned.await.err().expect("oversized return was delivered");
                            let trigger_error = trigger.await.expect_err("oversized return kept its channel usable");
                            assert!(matches!(
                                (props, error.error()),
                                (false, failure::Error::MessageTooLarge { size: 9, max: 8 })
                                    | (true, failure::Error::Capacity(
                                        failure::wire::Capacity::ContentProperty { attempted: 20, capacity: 19 }
                                    ))
                            ));
                            let (return_cause, method_cause,) = match (&error, &trigger_error) { (
                                failure::Runtime::Terminal(return_cause),
                                failure::Runtime::Terminal(method_cause),
                            ) => Some((return_cause, method_cause,)), _ => None, }.expect("transport failure was materialized locally");
                            assert!(
                                return_cause.is_same(method_cause),
                                "one transport generation produced more than one terminal cause"
                            );
                        })).is_ok(), "AMQP fiber must complete");
                    }, recover,
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
        .expect("spawn body-capacity broker");
    }
}

#[test]
fn wakes_fitting_channels() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            let channels: [u16; 3] = array::from_fn(|index| {
                let open = Method::read(stream).expect("channel.open");
                let channel = open.channel();
                assert_eq!(usize::from(channel), index + 1);
                open.check(channel, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                            .expect("AMQP frame fixture"),
                    )
                    .expect("channel open ok");
                channel
            });
            let [first, second, third] = channels;
            assert_ne!(first, second);
            assert_ne!(first, third);
            assert_ne!(second, third);
            let ready = Method::read(stream).expect("AMQP method");
            ready.check(first, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&ready).expect("AMQP fixture"), "ready");
            stream
                .write_all(
                    &Method::try_encode(first, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("ready purge ok");
            let trigger = Method::read(stream).expect("AMQP method");
            trigger.check(first, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&trigger).expect("AMQP fixture"), "trigger");
            stream
                .write_all(&returned_body(first, &[&[0_u8; 6]]).expect("AMQP fixture"))
                .expect("first return");
            stream
                .write_all(&returned_body(second, &[&[0_u8; 2]]).expect("AMQP fixture"))
                .expect("second return");
            stream
                .write_all(&returned_body(third, &[&[0_u8; 4]]).expect("AMQP fixture"))
                .expect("third return");
            stream
                .write_all(
                    &Method::try_encode(first, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("trigger purge ok");
            Method::read(stream)
                .expect("connection.close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection.close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .body(config::Body::default().max_size(8).buffer(6)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = wait_ready(conn).await.expect("ready");
                            let first = conn.channels().open(1).await.expect("first channel");
                            let second = conn.channels().open(2).await.expect("second channel");
                            let third = conn.channels().open(3).await.expect("third channel");
                            let mut first_return = Pinned::new(first.next_return());
                            assert!(poll_once(first_return.as_pin()).await.is_pending());
                            let mut second_return = Pinned::new(second.next_return());
                            assert!(poll_once(second_return.as_pin()).await.is_pending());
                            let mut third_return = Pinned::new(third.next_return());
                            assert!(poll_once(third_return.as_pin()).await.is_pending());
                            first.queue().purge("ready").await.expect("ready purge");
                            let mut trigger = Pinned::new(first.queue().purge("trigger"));
                            assert!(poll_once(trigger.as_pin()).await.is_pending());
                            let first_return = first_return
                                .await
                                .expect("first return operation")
                                .expect("first return");
                            assert!(first_return.body() == [0_u8; 6][..]);
                            first_return.release();
                            let second_return = second_return
                                .await
                                .expect("second return operation")
                                .expect("second return");
                            assert!(second_return.body() == [0_u8; 2][..]);
                            let third_return = third_return
                                .await
                                .expect("third return operation")
                                .expect("third return");
                            assert!(third_return.body() == [0_u8; 4][..]);
                            second_return.release();
                            third_return.release();
                            trigger.await.expect("trigger purge");
                            conn.control()
                                .close(ready, 200, "OK")
                                .await
                                .expect("close");
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
            broker.finish().expect("broker");
        },
    )
    .expect("spawn aggregate-body broker");
}

#[test]
fn wakes_property_waiter() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            let channels: [u16; 2] = array::from_fn(|index| {
                let open = Method::read(stream).expect("channel.open");
                let channel = open.channel();
                assert_eq!(usize::from(channel), index + 1);
                open.check(channel, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                            .expect("AMQP frame fixture"),
                    )
                    .expect("channel open ok");
                channel
            });
            let [first, second] = channels;
            assert_ne!(first, second);
            let ready = Method::read(stream).expect("AMQP method");
            ready.check(first, 50, 30).expect("AMQP method route");
            assert_eq!(
                queue_name(&ready).expect("AMQP fixture"),
                "ready-properties"
            );
            stream
                .write_all(
                    &Method::try_encode(first, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("ready purge ok");
            let trigger = Method::read(stream).expect("AMQP method");
            trigger.check(first, 50, 30).expect("AMQP method route");
            assert_eq!(
                queue_name(&trigger).expect("AMQP fixture"),
                "trigger-properties"
            );
            stream
                .write_all(&returned_properties(first, b"abc", &[]).expect("AMQP fixture"))
                .expect("first return");
            stream
                .write_all(&returned_properties(second, b"def", &[b"x"]).expect("AMQP fixture"))
                .expect("second return");
            stream
                .write_all(
                    &Method::try_encode(first, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("trigger purge ok");
            Method::read(stream)
                .expect("connection.close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection.close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .body(config::Body::default().buffer(RETURNED_PROPERTY_BYTES)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = wait_ready(conn).await.expect("ready");
                        let first = conn.channels().open(1).await.expect("first channel");
                        let second = conn.channels().open(2).await.expect("second channel");
                        let mut first_return = Pinned::new(first.next_return());
                        assert!(poll_once(first_return.as_pin()).await.is_pending());
                        let mut second_return = Pinned::new(second.next_return());
                        assert!(poll_once(second_return.as_pin()).await.is_pending());
                        first
                            .queue()
                            .purge("ready-properties")
                            .await
                            .expect("ready purge");
                        let mut trigger = Pinned::new(first.queue().purge("trigger-properties"));
                        assert!(poll_once(trigger.as_pin()).await.is_pending());
                        let first_return = first_return
                            .await
                            .expect("first return operation")
                            .expect("first return");
                        let properties = first_return.properties();
                        assert_eq!(properties.content_type(), Some("abc"));
                        let mut headers = properties.headers().expect("first headers").iter();
                        assert!(matches!(headers.next(), Some(("x", view::Value::String(b"abc")))));
                        assert!(headers.next().is_none());
                        assert!(poll_once(second_return.as_pin()).await.is_pending());
                        first_return.release();
                        let second_return = second_return
                            .await
                            .expect("second return operation")
                            .expect("second return");
                        assert!(second_return.body() == b"x"[..]);
                        let properties = second_return.properties();
                        assert_eq!(properties.content_type(), Some("def"));
                        let mut headers = properties.headers().expect("second headers").iter();
                        assert!(matches!(headers.next(), Some(("x", view::Value::String(b"def")))));
                        assert!(headers.next().is_none());
                        second_return.release();
                        trigger.await.expect("trigger purge");
                        conn.control()
                            .close(ready, 200, "OK")
                            .await
                            .expect("close");
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
            broker.finish().expect("broker");
        },
    )
    .expect("spawn property-capacity broker");
}

#[test]
fn reuses_fragment_pool() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");
            let ready = Method::read(stream).expect("AMQP method");
            ready.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&ready).expect("AMQP fixture"), "first-ready");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("first ready purge ok");
            stream
                .write_all(&returned_body(1, &[b"a", b"b"]).expect("AMQP fixture"))
                .expect("first return");
            let barrier = Method::read(stream).expect("AMQP method");
            barrier.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&barrier).expect("AMQP fixture"), "barrier");
            let mut frames = returned_body(1, &[b"c", b"d"]).expect("AMQP fixture");
            frames.extend_from_slice(
                &Method::try_encode(1, 50, 31, &0u32.to_be_bytes()).expect("AMQP frame fixture"),
            );
            stream
                .write_all(&frames)
                .expect("second return and barrier");
            let done = Method::read(stream).expect("AMQP method");
            done.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&done).expect("AMQP fixture"), "done");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("done purge ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default()
                            .body(config::Body::default().buffer(4).fragments(1)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            wait_ready(conn).await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let mut first = Pinned::new(channel.next_return());
                            assert!(poll_once(first.as_pin()).await.is_pending());
                            channel
                                .queue()
                                .purge("first-ready")
                                .await
                                .expect("first readiness barrier");
                            let first = first
                                .await
                                .expect("first return operation")
                                .expect("first return");
                            assert!(first.body() == b"ab"[..]);
                            let mut chunks = first.body().chunks();
                            assert_eq!(chunks.next(), Some(&b"a"[..]));
                            assert_eq!(chunks.next(), Some(&b"b"[..]));
                            assert_eq!(chunks.next(), None);
                            assert_eq!(first.reply_text(), "NO_ROUTE");
                            assert_eq!(first.exchange(), "");
                            assert_eq!(first.routing_key(), "");
                            drop(chunks);
                            let mut second = Pinned::new(channel.next_return());
                            assert!(poll_once(second.as_pin()).await.is_pending());
                            let mut barrier = Pinned::new(channel.queue().purge("barrier"));
                            assert!(poll_once(barrier.as_pin()).await.is_pending());
                            assert!(poll_once(second.as_pin()).await.is_pending());
                            first.release();
                            let second = second
                                .await
                                .expect("second return operation")
                                .expect("second return");
                            barrier.await.expect("fragment release barrier");
                            assert!(second.body() == b"cd"[..]);
                            let mut chunks = second.body().chunks();
                            assert_eq!(chunks.next(), Some(&b"c"[..]));
                            assert_eq!(chunks.next(), Some(&b"d"[..]));
                            assert_eq!(chunks.next(), None);
                            assert_eq!(second.reply_text(), "NO_ROUTE");
                            channel
                                .queue()
                                .purge("done")
                                .await
                                .expect("completion barrier");
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
            broker.finish().expect("broker");
        },
    )
    .expect("spawn body-fragment broker");
}

#[test]
fn rejects_fragment_exhaustion() {
    for (body, size, capacity) in [
        (config::Body::new(2, 2, 0), 2, 0),
        (config::Body::default(), 4098, 4096),
    ] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("handshake");
                let channel = returned::open_channel(stream).expect("channel");
                Method::read(stream)
                    .expect("trigger")
                    .check(channel, 50, 30)
                    .expect("trigger route");
                let chunks = vec![b"x".as_slice(); size];
                stream
                    .write_all(&returned_body(channel, &chunks).expect("frames"))
                    .expect("fragmented return");
                stream.read_wait(None).expect("wait for client close");
                match stream.read(&mut [0]) {
                    Ok(0) => {}
                    Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
                    result => panic!("client did not close: {result:?}"),
                }
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest")
                        .expect("config")
                        .capacity(config::Capacity::default().body(body)),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            wait_ready(conn).await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            let mut returned = Pinned::new(channel.next_return());
                            assert!(poll_once(returned.as_pin()).await.is_pending());
                            let mut trigger = Pinned::new(channel.queue().purge("trigger"));
                            assert!(poll_once(trigger.as_pin()).await.is_pending());
                            let error = returned.await.err().expect("fragment failure");
                            assert!(matches!(
                                error.error(),
                                failure::Error::Capacity(
                                    failure::wire::Capacity::BodyFragments { capacity: actual }
                                ) if actual.get() == capacity
                            ));
                            let trigger = trigger.await.expect_err("method failure");
                            let (failure::Runtime::Terminal(cause), failure::Runtime::Terminal(other)) =
                                (&error, &trigger)
                            else {
                                panic!("fragment failure was not terminal");
                            };
                            assert!(cause.is_same(other));
                        }))
                        .expect("fragment failure must wake the client");
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
        .expect("fragment broker");
    }
}

#[test]
fn rejects_fragment_cycle() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            let first = returned::open_channel(stream).expect("first channel");
            let second = returned::open_channel(stream).expect("second channel");
            Method::read(stream)
                .expect("trigger")
                .check(first, 50, 30)
                .expect("trigger route");
            let mut frames = Vec::new();
            for channel in [first, second] {
                frames.extend_from_slice(&returned::method(channel, "", "").expect("return"));
                frames.extend_from_slice(&basic::content_header(channel, 3).expect("header"));
            }
            for (channel, chunk) in [
                (first, b"a"),
                (first, b"b"),
                (second, b"c"),
                (second, b"d"),
                (first, b"e"),
                (second, b"f"),
            ] {
                frames.extend_from_slice(&Frame::try_encode(3, channel, chunk).expect("body"));
            }
            stream.write_all(&frames).expect("interleaved returns");
            stream.read_wait(None).expect("wait for client close");
            match stream.read(&mut [0]) {
                Ok(0) => {}
                Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
                result => panic!("client did not close: {result:?}"),
            }
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(config::Capacity::default().body(config::Body::new(3, 6, 1))),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        wait_ready(conn).await.expect("ready");
                        let first = conn.channels().open(1).await.expect("first channel");
                        let second = conn.channels().open(2).await.expect("second channel");
                        let mut left = Pinned::new(first.next_return());
                        assert!(poll_once(left.as_pin()).await.is_pending());
                        let mut right = Pinned::new(second.next_return());
                        assert!(poll_once(right.as_pin()).await.is_pending());
                        let mut trigger = Pinned::new(first.queue().purge("trigger"));
                        assert!(poll_once(trigger.as_pin()).await.is_pending());
                        let error = left.await.err().expect("first failure");
                        assert!(matches!(
                            error.error(),
                            failure::Error::Capacity(
                                failure::wire::Capacity::BodyFragments { capacity }
                            ) if capacity.get() == 1
                        ));
                        let second = right.await.err().expect("second failure");
                        let trigger = trigger.await.expect_err("method failure");
                        let failure::Runtime::Terminal(cause) = error else {
                            panic!("fragment failure was not terminal");
                        };
                        for error in [second, trigger] {
                            let failure::Runtime::Terminal(other) = error else {
                                panic!("fragment failure was not terminal");
                            };
                            assert!(cause.is_same(&other));
                        }
                    }))
                    .expect("fragment cycle must wake both channels");
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
    .expect("fragment-cycle broker");
}

#[test]
fn rejects_resident_cycle() {
    for (count, bytes, spare) in [(1, 4096_u32, 0), (2, 2000, 96)] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("handshake");
                for channel in [1, 2].into_iter().take(count) {
                    assert_eq!(returned::open_channel(stream).expect("channel"), channel);
                }
                Method::read(stream)
                    .expect("trigger")
                    .check(1, 50, 30)
                    .expect("trigger route");
                let mut props = (bytes - 4).to_be_bytes().to_vec();
                props.extend_from_slice(&[1, b'x', b'S']);
                props.extend_from_slice(&(bytes - 11).to_be_bytes());
                props.resize(bytes as usize, b'x');
                let mut frames = Vec::new();
                for channel in [1, 2].into_iter().take(count) {
                    frames.extend_from_slice(&returned::method(channel, "", "").expect("return"));
                    frames.extend_from_slice(
                        &basic::content_header_with(channel, 1500, 1 << 13, &props)
                            .expect("property header"),
                    );
                }
                for channel in [1, 2].into_iter().take(count) {
                    frames.extend_from_slice(
                        &Frame::try_encode(3, channel, &[b'b'; 1500]).expect("body"),
                    );
                }
                stream.write_all(&frames).expect("resident contention");
                stream.read_wait(None).expect("wait for client close");
                match stream.read(&mut [0]) {
                    Ok(0) => {}
                    Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
                    result => panic!("client did not close: {result:?}"),
                }
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest")
                        .expect("config")
                        .capacity(
                            config::Capacity::default()
                                .channels(count)
                                .channel(config::Channel::new(1, 0, 0, 0, 2))
                                .body(config::Body::new(1500, 4096, 1)),
                        ),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            wait_ready(conn).await.expect("ready");
                            let first = conn.channels().open(1).await.expect("first channel");
                            let second = if count == 2 {
                                Some(conn.channels().open(2).await.expect("second channel"))
                            } else {
                                None
                            };
                            let mut left = Pinned::new(first.next_return());
                            assert!(poll_once(left.as_pin()).await.is_pending());
                            let mut right = second.as_ref().map(|channel| Pinned::new(channel.next_return()));
                            if let Some(right) = right.as_mut() {
                                assert!(poll_once(right.as_pin()).await.is_pending());
                            }
                            let mut trigger = Pinned::new(first.queue().purge("trigger"));
                            assert!(poll_once(trigger.as_pin()).await.is_pending());
                            let error = left.await.err().expect("resident failure");
                            let failure::Runtime::Terminal(cause) = error else {
                                panic!("resident failure was not terminal");
                            };
                            let failure::Error::Hold {
                                target: failure::wire::Allocation::Content,
                                error: hold::Error::Copy(resident::copy::Error::Budget(error)),
                            } = cause.error()
                            else {
                                panic!("unexpected resident cause: {:?}", cause.error());
                            };
                            let (need, free) = error.into_parts();
                            assert_eq!((need.get(), free), (1500, spare));
                            let second = match right {
                                Some(right) => Some(right.await.err().expect("second failure")),
                                None => None,
                            };
                            let trigger = trigger.await.expect_err("method failure");
                            for error in second.into_iter().chain([trigger]) {
                                let failure::Runtime::Terminal(other) = error else {
                                    panic!("resident failure was not terminal");
                                };
                                assert!(cause.is_same(&other));
                            }
                        }))
                        .expect("resident failure must wake the client");
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
        .expect("resident broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
