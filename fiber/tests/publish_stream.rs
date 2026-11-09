use dope_test::alloc;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Frame, Method};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use std::time::Duration;

use dope_amqp091::adapter::publish;
use dope_amqp091::config;
use dope_amqp091::message::property::Properties;
use dope_test::alloc::Tracker;
use dope_test::fibers::Yield;
use dope_test::harness::peer::AcceptScript;

use crate::common::fiber::{Pinned, poll_once};
use crate::common::publish::{TUNE, routing_key, with_config};

#[test]
fn stale_drop_keeps_stream() {
    AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            Method::write(&mut first, 1, 20, 11, &[0; 4]).expect("first channel.open-ok");
            let stale = Method::read(&mut first).expect("AMQP method");
            stale.check(1, 60, 40).expect("AMQP method route");
            assert_eq!(routing_key(&stale).expect("AMQP fixture"), "stale-stream");
            drop(first);

            let mut second = acceptor.accept().expect("recovered connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            Method::write(&mut second, 1, 20, 11, &[0; 4]).expect("replayed channel.open-ok");

            let current = Method::read(&mut second).expect("AMQP method");
            current.check(1, 60, 40).expect("AMQP method route");
            assert_eq!(
                routing_key(&current).expect("AMQP fixture"),
                "current-stream"
            );
            Method::write(&mut second, 1, 20, 20, &[0]).expect("pause publication");
            Method::write(&mut second, 0, 10, 60, b"\x05ready")
                .expect("current publication observed");

            let Frame(kind, channel, header) = Frame::read(&mut second).expect("content header");
            assert_eq!((kind, channel), (2, 1));
            let size = header[4..12]
                .try_into()
                .expect("content header size occupies eight octets");
            assert_eq!(u64::from_be_bytes(size), 3);
            let Frame(kind, channel, body) = Frame::read(&mut second).expect("content body");
            assert_eq!((kind, channel, body.as_slice()), (3, 1, b"new".as_slice()));

            let flow = Method::read(&mut second).expect("deferred flow reply");
            flow.check(1, 20, 21).expect("flow reply route");
            assert_eq!(&flow.1[4..], &[0]);

            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 50, 30).expect("AMQP method route");
            Method::write(&mut second, 1, 20, 20, &[1]).expect("resume publication");
            Method::write(&mut second, 0, 10, 61, &[]).expect("connection unblocked");
            Method::write(&mut second, 1, 50, 31, &[0; 4]).expect("queue.purge-ok");
            let flow = Method::read(&mut second).expect("idle flow reply");
            flow.check(1, 20, 21).expect("flow reply route");
            assert_eq!(&flow.1[4..], &[1]);
            Method::read(&mut second).expect("resumed publish").check(1, 60, 40)
                .expect("publish route");
            Method::write(&mut second, 1, 20, 20, &[0]).expect("pending flow");
            Method::write(&mut second, 1, 20, 20, &[1]).expect("flow capacity exceeded");
            assert_eq!(second.read(&mut [0]).expect("stream probe teardown"), 0);
        },
        |broker| {
            let cap = config::Capacity::default().egress(config::Egress::new(1, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);
            with_config(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                let target = conn.observe().ready().await.expect("first ready");
                let channel = conn
                    .channels()
                    .open_in(target, 1)
                    .await
                    .expect("channel open");
                let channel = channel.into_auto();
                let props = Properties::default();
                let mut body = *b"old";
                let mut stale = Pinned::new(channel.basic().publish(publish::Request {
                    exchange: "",
                    routing_key: "stale-stream",
                    mandatory: false,
                    properties: &props,
                    body: &body,
                }));
                assert!(poll_once(stale.as_pin()).await.is_pending());
                let retired = conn.observe().retired(target.epoch()).await;
                assert!(retired.is_ok(), "old transport must retire: {retired:?}");
                let ready = conn.observe().ready().await.expect("replayed topology must become ready");
                assert!(ready.epoch() != target.epoch(), "publication must use a new transport epoch");

                let mut current = Pinned::new(channel.basic().publish(publish::Request {
                    exchange: "",
                    routing_key: "current-stream",
                    mandatory: false,
                    properties: &props,
                    body: b"new",
                }));
                assert!(poll_once(current.as_pin()).await.is_pending());
                while !conn.observe().read(|view| view.is_blocked()).await {
                    Yield::new().await;
                }

                let (_, alloc) = Tracker::usage(|| drop(stale));
                assert_eq!(alloc, (0, 0));
                body.fill(0);
                current.await.expect("current publish");
                channel.queue().purge("barrier").await.expect("post-drop barrier");
                let mut resumed = Pinned::new(channel.basic().publish(
                    common::client::channel::request("", "resumed", &props, &[])));
                assert!(poll_once(resumed.as_pin()).await.is_pending());
                conn.observe().retired(ready.epoch()).await.expect("flow capacity retirement");
                let error = conn.observe().ready().await.expect_err("flow capacity failure");
                assert!(matches!(error.error(), dope_amqp091::failure::Error::Capacity(
                    dope_amqp091::failure::wire::Capacity::Method)));
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
                complete,
            );
            broker.finish().expect("stream reconnect broker");
        },
    ).expect("stream reconnect broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();
