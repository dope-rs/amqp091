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

use std::sync::mpsc;

use dope_amqp091::failure;
use dope_fiber::context;
use dope_test::fibers::Yield;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn drop_closes_before_reuse() {
    let (req_tx, req_rx) = mpsc::channel();
    let (drop_tx, drop_rx) = mpsc::channel();
    let (c1_tx, c1_rx) = mpsc::channel();
    let (r1_tx, r1_rx) = mpsc::channel();
    let (d1_tx, d1_rx) = mpsc::channel();
    let (c2_tx, c2_rx) = mpsc::channel();
    let (r2_tx, r2_rx) = mpsc::channel();
    let (d2_tx, d2_rx) = mpsc::channel();
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
            req_tx.send(()).expect("publish first open request");
            drop_rx.recv().expect("first open drop");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first open-ok");
            Method::read(stream).expect("AMQP method").check(1, 20, 40).expect("AMQP method route");
            c1_tx.send(()).expect("publish first close request");
            r1_rx.recv().expect("first reuse rejection");
            stream
                .write_all(&Method::try_encode(1, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("first close-ok");
            d1_tx.send(()).expect("publish first close completion");

            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first id reopen-ok");

            Method::read(stream).expect("AMQP method").check(2, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("second open-ok");
            stream
                .write_all(
                    &Method::try_encode(0, 10, 60, &[4, b'g', b'a', b't', b'e'])
                        .expect("AMQP frame fixture"),
                )
                .expect("second response barrier");

            Method::read(stream).expect("AMQP method").check(3, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(3, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first regular-lane filler open-ok");
            Method::read(stream).expect("AMQP method").check(4, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(4, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("second regular-lane filler open-ok");
            Method::read(stream).expect("AMQP method").check(2, 20, 40).expect("AMQP method route");
            c2_tx.send(()).expect("publish second close request");
            r2_rx.recv().expect("second reuse rejection");
            stream
                .write_all(&Method::try_encode(2, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("second close-ok");
            d2_tx.send(()).expect("publish second close completion");

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
                loop {
                    match req_rx.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        outcome => outcome.expect("broker ended before observing first open"),
                    }
                }
                let (_, alloc) = Tracker::usage(|| drop(first));
                assert_eq!(alloc, (0, 0));
                drop_tx.send(()).expect("publish first open drop");
                loop {
                    match c1_rx.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        outcome => outcome.expect("broker ended before first close request"),
                    }
                }
                let error = conn.channels().open(1).await.err().expect("first channel id reused before close-ok");
                assert!(matches!((error).error(), failure::Error::ChannelInUse(1)));
                r1_tx.send(()).expect("publish first reuse rejection");
                loop {
                    match d1_rx.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        outcome => outcome.expect("broker ended before first close completion"),
                    }
                }
                let _first = loop {
                    match conn.channels().open(1).await {
                        Ok(channel) => break channel,
                        Err(error) if matches!((error).error(), failure::Error::ChannelInUse(1)) => {
                            Yield::new().await;
                        }
                        outcome => break outcome.expect("first channel id reuse failed"),
                    }
                };

                let mut pending = Some(Pinned::new(conn.channels().open(2)));
                assert!(poll_once(pending.as_mut().expect("second open").as_pin()).await.is_pending());
                while !conn.observe().read(|view| view.is_blocked()).await {
                    Yield::new().await;
                }

                let mut third = Pinned::new(conn.channels().open(3));
                let mut fourth = Pinned::new(conn.channels().open(4));
                poll_fn(|mut context| {
                    for filler in [&mut third, &mut fourth] {
                        assert!(matches!(
                            context.as_mut().try_poll(filler.as_pin()),
                            context::PollResult::Polled(Poll::Pending)
                        ), "filler must remain pending within the poll budget");
                    }
                    let (_, alloc) = Tracker::usage(|| drop(pending.take()));
                    assert_eq!(alloc, (0, 0));
                    Poll::Ready(())
                })
                .await;
                let _third = third.await.expect("first filler channel");
                let _fourth = fourth.await.expect("second filler channel");

                loop {
                    match c2_rx.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        outcome => outcome.expect("broker ended before second close request"),
                    }
                }
                let error = conn.channels().open(2).await.err().expect("second channel id reused before close-ok");
                assert!(matches!((error).error(), failure::Error::ChannelInUse(2)));
                r2_tx.send(()).expect("publish second reuse rejection");
                loop {
                    match d2_rx.try_recv() {
                        Ok(()) => break,
                        Err(mpsc::TryRecvError::Empty) => Yield::new().await,
                        outcome => outcome.expect("broker ended before second close completion"),
                    }
                }
                let _second = loop {
                    match conn.channels().open(2).await {
                        Ok(channel) => break channel,
                        Err(error) if matches!((error).error(), failure::Error::ChannelInUse(2)) => {
                            Yield::new().await;
                        }
                        outcome => break outcome.expect("second channel id reuse failed"),
                    }
                };
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
