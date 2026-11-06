use std::convert::Infallible;
use std::task::Poll;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_fiber::abi::poll_fn;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use dope_amqp091::failure;
use dope_fiber::context;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn drop_closes_before_reuse() {
    TcpScript::with(
        move |stream| {
            stream
                .read_wait(Some(Duration::from_secs(10)))
                .expect("read timeout");
            stream
                .write_wait(Some(Duration::from_secs(10)))
                .expect("write timeout");
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");

            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            Method::read(stream).expect("observation channel")
                .check(4, 20, 10).expect("channel.open");
            Method::write(stream, 4, 20, 11, &[0; 4]).expect("open request observed");
            Method::read(stream).expect("post-drop purge")
                .check(4, 50, 30).expect("queue.purge");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first open-ok");
            Method::read(stream).expect("AMQP method").check(1, 20, 40).expect("AMQP method route");
            Method::write(stream, 4, 50, 31, &[0; 4]).expect("close request observed");
            Method::read(stream).expect("reuse rejection purge")
                .check(4, 50, 30).expect("queue.purge");
            stream
                .write_all(&Method::try_encode(1, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("first close-ok");
            Method::write(stream, 4, 50, 31, &[0; 4]).expect("first close processed");

            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first id reopen-ok");

            Method::read(stream).expect("AMQP method").check(2, 20, 10).expect("AMQP method route");
            Method::read(stream).expect("second response observation")
                .check(4, 50, 30).expect("queue.purge");
            stream
                .write_all(
                    &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("second open-ok");
            Method::write(stream, 4, 50, 31, &[0; 4]).expect("second response processed");

            Method::read(stream).expect("AMQP method").check(3, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(3, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first regular-lane filler open-ok");
            Method::read(stream).expect("second regular-lane filler")
                .check(4, 50, 30).expect("queue.purge");
            Method::read(stream).expect("AMQP method").check(2, 20, 40).expect("AMQP method route");
            Method::write(stream, 4, 50, 31, &[0; 4]).expect("second close request observed");
            Method::read(stream).expect("second reuse rejection purge")
                .check(4, 50, 30).expect("queue.purge");
            stream
                .write_all(&Method::try_encode(2, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("second close-ok");
            Method::write(stream, 4, 50, 31, &[0; 4]).expect("second close processed");

            Method::read(stream).expect("AMQP method").check(2, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("second id reopen-ok");
            Method::read(stream).expect("AMQP method").check(0, 10, 50).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(0)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(4)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                let ready = conn.observe().ready().await.expect("ready");

                let mut first = Pinned::new(conn.channels().open(1));
                assert!(poll_once(first.as_pin()).await.is_pending());
                let observer = conn.channels().open(4).await.expect("first open request observed");
                let (_, alloc) = Tracker::usage(|| drop(first));
                assert_eq!(alloc, (0, 0));
                observer.queue().purge("dropped").await.expect("first close request observed");
                let error = conn.channels().open(1).await.err().expect("first channel id reused before close-ok");
                assert!(matches!(error.error(), failure::Error::ChannelInUse(1)));
                observer.queue().purge("rejected").await.expect("first close processed");
                let _first = conn.channels().open(1).await.expect("first channel id reuse");

                let mut pending = Some(Pinned::new(conn.channels().open(2)));
                assert!(poll_once(pending.as_mut().expect("second open").as_pin()).await.is_pending());
                observer.queue().purge("replied").await.expect("second open response processed");

                let mut third = Pinned::new(conn.channels().open(3));
                let mut fourth = Pinned::new(observer.queue().purge("filler"));
                poll_fn(|mut context| {
                    assert!(matches!(
                        context.as_mut().try_poll(third.as_pin()),
                        context::PollResult::Polled(Poll::Pending)
                    ), "first filler must consume regular credit");
                    assert!(matches!(
                        context.as_mut().try_poll(fourth.as_pin()),
                        context::PollResult::Polled(Poll::Pending)
                    ), "second filler must consume regular credit");
                    let (_, alloc) = Tracker::usage(|| drop(pending.take()));
                    assert_eq!(alloc, (0, 0));
                    Poll::Ready(())
                })
                .await;
                let _third = third.await.expect("first filler channel");
                fourth.await.expect("second close request observed");

                let error = conn.channels().open(2).await.err().expect("second channel id reused before close-ok");
                assert!(matches!(error.error(), failure::Error::ChannelInUse(2)));
                observer.queue().purge("rejected").await.expect("second close processed");
                let _second = conn.channels().open(2).await.expect("second channel id reuse");
                conn
                    .control()
                    .close(ready, 200, "done")
                    .await
                    .expect("close");
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("open lifetime broker");
        },
    )
    .expect("open lifetime broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
