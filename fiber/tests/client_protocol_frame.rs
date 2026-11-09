use std::task::Poll;

use dope_amqp091::adapter::publish::Request;
use dope_amqp091::config;
use dope_amqp091::message::property::Properties;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Frame, Method};

use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_amqp091::failure;
use dope_amqp091::protocol::method;
use dope_amqp091::protocol::wire::frame;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;
use crate::common::fiber::{Pinned, poll_once};

#[test]
fn rejects_partial_response_before_reusing_method() {
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream).expect("open").check(1, 20, 10).expect("open route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream).expect("publish channel").check(2, 20, 10).expect("open route");
            Method::write(stream, 2, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream).expect("first purge").check(1, 50, 30).expect("purge route");
            let extra = Method::try_encode(1, 50, 31, &299u32.to_be_bytes()).expect("extra reply");
            let mut prefix = Method::try_encode(1, 50, 31, &7u32.to_be_bytes()).expect("first reply");
            prefix.extend_from_slice(&extra[..extra.len() - 1]);
            stream.write_all(&prefix).expect("complete first and partial extra");
            let mut next = Method::read(stream).expect("request after second poll");
            if next.channel() == 1 {
                next.check(1, 50, 30).expect("second purge route");
                next = Method::read(stream).expect("publish after second poll");
            }
            next.check(2, 60, 40).expect("publish route");
            Frame::read(stream).expect("empty publication").check(2, 2)
                .expect("publish header");
            let mut tail = extra[extra.len() - 1..].to_vec();
            tail.extend_from_slice(&Method::try_encode(1, 50, 31, &11u32.to_be_bytes()).expect("second reply"));
            stream.write_all(&tail).expect("extra suffix and second reply");
            while stream.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let publisher = conn.channels().open(2).await.expect("publish channel");
                        let first = channel.queue().purge("first").await.expect("first response");
                        assert_eq!(first.message_count, 7);
                        let mut second = Pinned::new(channel.queue().purge("second"));
                        let polled = poll_once(second.as_pin()).await;
                        publisher.basic().publish(Request {
                            exchange: "", routing_key: "", mandatory: false,
                            properties: &Properties::default(), body: &[],
                        }).await.expect("publish while response admission is held");
                        let second = match polled {
                            Poll::Pending => second.await,
                            Poll::Ready(result) => result,
                        };
                        let error = second.expect_err("an unsolicited partial response cannot complete the next method");
                        assert!(matches!(error.error(), failure::Error::Protocol(
                            failure::wire::Fault::ChanResponse {
                                channel: 1,
                                expected: None,
                                actual: method::Kind::QueuePurge,
                            }
                        )), "{:?}", error.error());
                    })).expect("fiber");
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    ).expect("scripted broker");
}

#[test]
fn rejects_partial_close_reply_before_close_admission() {
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream).expect("open").check(1, 20, 10).expect("open route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream).expect("publish channel").check(2, 20, 10).expect("open route");
            Method::write(stream, 2, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream).expect("purge").check(1, 50, 30).expect("purge route");
            let close = Method::try_encode(0, 10, 51, &[]).expect("close reply");
            let mut prefix = Method::try_encode(1, 50, 31, &7u32.to_be_bytes()).expect("purge reply");
            prefix.extend_from_slice(&close[..2]);
            stream.write_all(&prefix).expect("complete purge and partial close reply");
            Method::read(stream).expect("publish after close poll").check(2, 60, 40)
                .expect("publish route");
            Frame::read(stream).expect("empty publication").check(2, 2)
                .expect("publish header");
            stream.write_all(&close[2..]).expect("close reply suffix");
            while stream.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let publisher = conn.channels().open(2).await.expect("publish channel");
                        channel.queue().purge("first").await.expect("purge response");
                        let mut close = Pinned::new(conn.control().close(ready, 200, "OK"));
                        let polled = poll_once(close.as_pin()).await;
                        publisher.basic().publish(Request {
                            exchange: "", routing_key: "", mandatory: false,
                            properties: &Properties::default(), body: &[],
                        }).await.expect("publish while close admission is held");
                        let close = match polled {
                            Poll::Pending => close.await,
                            Poll::Ready(result) => result,
                        };
                        let error = close.expect_err("an earlier partial reply cannot acknowledge a new close");
                        assert!(matches!(error.error(), failure::Error::Protocol(
                            failure::wire::Fault::ConnMethod {
                                method: failure::wire::ConnMethod::CloseOk,
                                phase: failure::wire::ConnPhase::Ready,
                                closing: false,
                            }
                        )), "{:?}", error.error());
                    })).expect("fiber");
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    ).expect("scripted broker");
}

#[test]
fn rejects_invalid_frames() {
    for zero_tag in [false, true] {
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
            let trigger = Method::read(stream).expect("AMQP method");
            trigger.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&trigger).expect("AMQP fixture"), "trigger");
            let frame = if zero_tag {
                common::basic::delivery::method(1, "consumer", 0)
                    .expect("zero delivery tag fixture")
            } else {
                let mut header = vec![1, 0, 0];
                header.extend_from_slice(&131_072u32.to_be_bytes());
                header.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0xce]);
                header
            };
            stream.write_all(&frame).expect("invalid frame");
            if !zero_tag {
                let close = Method::read(stream).expect("frame error close");
                close.check(0, 10, 50).expect("connection close");
                assert_eq!(&close.1[4..6], &501u16.to_be_bytes());
                assert_eq!(stream.read(&mut [0]).expect("closed input"), 0);
            }
        },
        |broker| {
            let addr = broker.addr();
            with(
                addr,
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let error = channel.queue().purge("trigger").await
                                .expect_err("invalid frame must fail the pending request");
                            assert!(matches!((zero_tag, error.error()),
                                (false, failure::Error::Frame(
                                    frame::Fault::FrameTooLarge {
                                    size: 131_080,
                                    max: 131_072,
                                    }
                                ))
                                | (true, failure::Error::Protocol(failure::wire::Fault::DeliveryTagZero))
                            ), "{:?}", error.error());
                            if !zero_tag {
                                conn.observe().retired(ready.epoch()).await.expect("retirement");
                                conn.observe().read(|view| {
                                    assert_eq!(view.last_exit().expect("frame error exit").reason(), failure::Reason::Protocol);
                                }).await;
                            }
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn invalid-frame broker");
    }
}
