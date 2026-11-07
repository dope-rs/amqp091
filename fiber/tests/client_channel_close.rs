use std::task::Poll;
use std::time::Duration;

use dope_amqp091::config;
use dope_fiber::abi::poll_fn;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Frame, Method, put_short};
use crate::common::basic::{self, returned};
use crate::common::fiber::{Pinned, poll_once};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_amqp091::failure;
use dope_fiber::context;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

const CLOSE_EGRESS_CAPACITY: usize = 4;
const CLOSE_CHANNEL_COUNT: u16 = CLOSE_EGRESS_CAPACITY as u16 + 1;

#[test]
fn close_drains_or_releases_pending_rpc() {
    for cancel in [false, true] {
        TcpScript::with(
            move |stream| {
                stream.read_wait(Some(Duration::from_secs(10))).expect("read deadline");
                Peer::new(stream).open(TUNE).expect("handshake");
                Method::read(stream).expect("channel open")
                    .check(1, 20, 10).expect("channel.open");
                Method::write(stream, 1, 20, 11, &[0, 0, 0, 0]).expect("channel.open-ok");
                let first = Method::read(stream).expect("first purge");
                first.check(1, 50, 30).expect("queue.purge");
                assert_eq!(queue_name(&first).expect("queue name"), "first");
                check_no_frame(stream, "RPC ahead of pending purge").expect("RPC ordering");
                Method::write(stream, 1, 50, 31, &0_u32.to_be_bytes()).expect("first purge-ok");
                let next = Method::read(stream).expect("operation after purge");
                if cancel {
                    next.check(1, 50, 30).expect("cancelled close emitted a frame");
                    assert_eq!(queue_name(&next).expect("queue name"), "next");
                    Method::write(stream, 1, 50, 31, &0_u32.to_be_bytes()).expect("next purge-ok");
                } else {
                    next.check(1, 20, 40).expect("channel.close");
                    Method::write(stream, 1, 20, 41, &[]).expect("channel.close-ok");
                }
                Method::read(stream).expect("connection close")
                    .check(0, 10, 50).expect("stray channel operation");
                Method::write(stream, 0, 10, 51, &[]).expect("connection.close-ok");
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest").expect("config").capacity(
                        config::Capacity::default().channels(1)
                            .channel(config::Channel::default().methods(2)),
                    ),
                    |rt| {
                        let conn = rt.connection();
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel");
                            let mut first = Pinned::new(channel.queue().purge("first"));
                            let mut close = Some(Pinned::new(channel.close(200, "done")));
                            let mut next = Pinned::new(channel.queue().purge("next"));
                            poll_fn(|mut context| {
                                assert!(matches!(
                                    context.as_mut().try_poll(first.as_pin()),
                                    context::PollResult::Polled(Poll::Pending)
                                ));
                                assert!(matches!(
                                    context.as_mut().try_poll(close.as_mut().expect("close").as_pin()),
                                    context::PollResult::Polled(Poll::Pending)
                                ));
                                if cancel {
                                    drop(close.take());
                                    assert!(matches!(
                                        context.as_mut().try_poll(next.as_pin()),
                                        context::PollResult::Polled(Poll::Pending)
                                    ));
                                } else {
                                    assert!(matches!(
                                        context.as_mut().try_poll(next.as_pin()),
                                        context::PollResult::Polled(Poll::Ready(Err(error)))
                                            if matches!(error.error(), failure::Error::Closed)
                                    ));
                                }
                                Poll::Ready(())
                            }).await;
                            first.await.expect("first purge");
                            if cancel {
                                next.await.expect("purge after cancelled close");
                            } else {
                                close.expect("retained close").await.expect("close after purge");
                            }
                            conn.control().close(ready, 200, "done").await.expect("close connection");
                        })).expect("fiber completion");
                    },
                    recover,
                    complete,
                );
                broker.finish().expect("broker");
            },
        ).expect("close ordering broker");
    }
}

#[test]
fn cancelled_unsent_close_preserves_inbound_content() {
    TcpScript::with(
        |stream| {
            stream.read_wait(Some(Duration::from_secs(10))).expect("read deadline");
            Peer::new(stream).open(TUNE).expect("handshake");
            let channel = returned::open_channel(stream).expect("channel");
            let barrier = returned::open_channel(stream).expect("barrier channel");
            Method::read(stream).expect("pending purge").check(channel, 50, 30)
                .expect("purge method");
            Method::read(stream).expect("partial barrier").check(barrier, 50, 30)
                .expect("barrier method");
            stream.write_all(&returned::method(channel, "", "").expect("return method"))
                .expect("partial return");
            stream.write_all(&basic::content_header(channel, 2).expect("return header"))
                .expect("partial header");
            stream.write_all(&Frame::try_encode(3, channel, b"a").expect("body frame"))
                .expect("partial body");
            Method::write(stream, barrier, 50, 31, &[0; 4]).expect("partial barrier reply");
            Method::read(stream).expect("closing barrier").check(barrier, 50, 30)
                .expect("unsent close barrier");
            stream.write_all(&Frame::try_encode(3, channel, b"b").expect("body frame"))
                .expect("remaining body");
            stream.write_all(&returned::content(channel, "", "", &[b"c"], None)
                .expect("return frames")).expect("next return");
            Method::write(stream, channel, 50, 31, &[0; 4]).expect("pending purge reply");
            Method::write(stream, barrier, 50, 31, &[0; 4]).expect("closing barrier reply");
            Method::read(stream).expect("purge after cancellation").check(channel, 50, 30)
                .expect("cancelled close emitted a frame");
            Method::write(stream, channel, 50, 31, &[0; 4]).expect("next purge reply");
            returned::close_connection(stream).expect("connection close");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config").capacity(
                    config::Capacity::default().channels(2)
                        .channel(config::Channel::default().methods(2).returns(2)),
                ),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let barrier = conn.channels().open(2).await.expect("barrier channel");
                        let mut first = Pinned::new(channel.queue().purge("pending"));
                        assert!(poll_once(first.as_pin()).await.is_pending());
                        barrier.queue().purge("partial").await.expect("partial content barrier");
                        let mut close = Pinned::new(channel.close(200, "done"));
                        assert!(poll_once(close.as_pin()).await.is_pending());
                        let mut content = Pinned::new(barrier.queue().purge("closing"));
                        assert!(poll_once(content.as_pin()).await.is_pending());
                        let message = channel.next_return().await.expect("return during unsent close")
                            .expect("completed partial return");
                        assert!(message.body().chunks().flatten().eq(b"ab"));
                        message.release();
                        content.await.expect("closing content barrier");
                        drop(close);
                        channel.queue().purge("next").await.expect("purge after cancelled close");
                        first.await.expect("pending purge");
                        let message = channel.next_return().await.expect("return after cancelled close")
                            .expect("retained return");
                        assert!(message.body().chunks().flatten().eq(b"c"));
                        message.release();
                        conn.control().close(ready, 200, "done").await.expect("connection close");
                    })).expect("fiber completion");
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    ).expect("content close broker");
}

#[test]
fn waits_for_channel_credit() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            for channel in 1..=CLOSE_CHANNEL_COUNT {
                Method::read(stream)
                    .expect("AMQP method")
                    .check(channel, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                            .expect("AMQP frame fixture"),
                    )
                    .expect("channel open ok");
            }
            let mut purged = [false; CLOSE_EGRESS_CAPACITY + 1];
            for _ in 0..CLOSE_EGRESS_CAPACITY {
                let frame = Method::read(stream).expect("AMQP method");
                let channel = usize::from(frame.0);
                assert!((1..=CLOSE_EGRESS_CAPACITY).contains(&channel));
                assert!(!purged[channel], "duplicate purge for channel {channel}");
                assert_eq!(
                    &frame.1[..4],
                    &[50u16.to_be_bytes(), 30u16.to_be_bytes()].concat()
                );
                purged[channel] = true;
            }
            for channel in 1..=CLOSE_EGRESS_CAPACITY as u16 {
                stream
                    .write_all(
                        &Method::try_encode(channel, 50, 31, &0u32.to_be_bytes())
                            .expect("AMQP frame fixture"),
                    )
                    .expect("purge ok");
            }
            Method::read(stream)
                .expect("AMQP method")
                .check(CLOSE_CHANNEL_COUNT, 20, 40).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(CLOSE_CHANNEL_COUNT, 20, 41, &[])
                        .expect("AMQP frame fixture"),
                )
                .expect("channel close ok");
        },
        |broker| {
            let addr = broker.addr();
            with(
                addr,
                config::Config::new("guest", "guest")
                    .expect("config")
                    .tune(config::Tune::default().channel_max(16))
                    .capacity(
                        config::Capacity::default()
                            .channels(usize::from(CLOSE_CHANNEL_COUNT))
                            .egress(config::Egress::new(CLOSE_EGRESS_CAPACITY, 2048)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let mut channels = Vec::with_capacity(usize::from(CLOSE_CHANNEL_COUNT));
                        for channel in 1..=CLOSE_CHANNEL_COUNT {
                            channels.push(
                                conn.channels().open(channel)
                                    .await
                                    .expect("open channel"),
                            );
                        }
                        let mut purges = channels[..CLOSE_EGRESS_CAPACITY]
                            .iter()
                            .map(|channel| Pinned::new(channel.queue().purge("fill")))
                            .collect::<Vec<_>>();
                        let mut close =
                            Pinned::new(channels[CLOSE_EGRESS_CAPACITY].close(200, "OK"));
                        let mut admitted = 0;
                        poll_fn(|mut cx| {
                            while admitted < purges.len() {
                                let outcome = match cx.as_mut().try_poll(purges[admitted].as_pin()) {
                                    context::PollResult::Polled(outcome) => outcome,
                                    context::PollResult::Exhausted => return Poll::Pending,
                                };
                                assert!(outcome.is_pending());
                                admitted += 1;
                            }
                            let outcome = match cx.as_mut().try_poll(close.as_pin()) {
                                context::PollResult::Polled(outcome) => outcome,
                                context::PollResult::Exhausted => return Poll::Pending,
                            };
                            assert!(outcome.is_pending(), "close bypassed exhausted outbound credit");
                            Poll::Ready(())
                        })
                        .await;
                        for purge in purges {
                            purge.await.expect("purge");
                        }
                        close.await.expect("close after outbound credit release");
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn close-capacity broker");
}

#[test]
fn waits_for_conn_credit() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            for channel in 1..=2 {
                Method::read(stream)
                    .expect("AMQP method")
                    .check(channel, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])
                            .expect("AMQP frame fixture"),
                    )
                    .expect("channel open ok");
            }
            let mut filled = [false; 3];
            for _ in 0..2 {
                let purge = Method::read(stream).expect("AMQP method");
                let channel = usize::from(purge.0);
                purge.check(purge.0, 50, 30).expect("AMQP method route");
                assert!((1..=2).contains(&channel));
                assert_eq!(
                    queue_name(&purge).expect("AMQP fixture"),
                    format!("fill-{channel}")
                );
                filled[channel] = true;
            }
            for channel in 1..=2 {
                assert!(filled[channel as usize]);
                stream
                    .write_all(
                        &Method::try_encode(channel, 50, 31, &0u32.to_be_bytes())
                            .expect("AMQP frame fixture"),
                    )
                    .expect("purge ok");
            }
            Method::read(stream).expect("AMQP method").check(0, 10, 50).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close ok");
        },
        |broker| {
            let addr = broker.addr();
            with(
                addr,
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(config::Capacity::default().egress(config::Egress::new(2, 2048))),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let first = conn.channels().open(1).await.expect("open first channel");
                            let second = conn.channels().open(2).await.expect("open second channel");
                            let mut first_fill = Pinned::new(first.queue().purge("fill-1"));
                            let mut second_fill = Pinned::new(second.queue().purge("fill-2"));
                            let mut close = Pinned::new(conn.control().close(ready, 200, "done"));
                            poll_fn(|mut cx| {
                                for fill in [&mut first_fill, &mut second_fill] {
                                    let outcome = match cx.as_mut().try_poll(fill.as_pin()) {
                                        context::PollResult::Polled(outcome) => outcome,
                                        context::PollResult::Exhausted => return Poll::Pending,
                                    };
                                    assert!(outcome.is_pending());
                                }
                                let outcome = match cx.as_mut().try_poll(close.as_pin()) {
                                    context::PollResult::Polled(outcome) => outcome,
                                    context::PollResult::Exhausted => return Poll::Pending,
                                };
                                assert!(
                                    outcome.is_pending(),
                                    "connection close bypassed exhausted outbound credit"
                                );
                                Poll::Ready(())
                            })
                            .await;
                            first_fill.await.expect("first fill purge");
                            second_fill.await.expect("second fill purge");
                            close.await.expect("close after outbound credit release");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                }, recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn connection-close broker");
}

#[test]
fn server_is_terminal() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel open ok");
            Method::read(stream).expect("AMQP method").check(1, 50, 30).expect("AMQP method route");
            let mut close = Vec::new();
            close.extend_from_slice(&404u16.to_be_bytes());
            put_short(&mut close, b"closed").expect("AMQP short string");
            close.extend_from_slice(&50u16.to_be_bytes());
            close.extend_from_slice(&30u16.to_be_bytes());
            stream
                .write_all(&Method::try_encode(1, 20, 40, &close).expect("AMQP frame fixture"))
                .expect("server channel close");
            Method::read(stream).expect("AMQP method").check(1, 20, 41).expect("AMQP method route");
            let mut byte = [0_u8; 1];
            assert_eq!(
                stream.read(&mut byte).expect("transport retirement"),
                0,
                "Channel.CloseOk must drain before the unrecoverable transport retires"
            );
        },
        |broker| {
            let addr = broker.addr();
            with(
                addr,
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(config::Capacity::default().channels(1)),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open_auto().await.expect("open channel");
                            let error = channel.queue().purge("pending").await.expect_err("terminal channel accepted a purge");
                            assert!(matches!((error).error(), failure::Error::Server {
                                    reply_code: 404,
                                    ..
                                }));
                            let reopen = conn.channels().open_auto().await.err().expect("terminal channel close reopened");
                            assert!(matches!((reopen).error(), failure::Error::Server {
                                    reply_code: 404,
                                    ..
                                }));
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                }, recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn server-channel-close broker");
}
