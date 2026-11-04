use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
pub mod common;

use common::basic::returned;
use common::client::{TUNE, returned_body, with};
use common::fiber::{Pinned, poll_once};
use dope_amqp091::config;
use dope_test::alloc::Tracker;
use dope_test::harness::peer::TcpScript;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn preserves_owners() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream).expect("open").check(1, 20, 10).expect("AMQP method route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("open reply");
            for (index, chunks) in [
                &[b"ab".as_slice(), b"cd".as_slice()][..],
                &[b"ef".as_slice(), b"gh".as_slice(), b"ij".as_slice()][..],
                &[b"kl".as_slice()][..],
            ].into_iter().enumerate() {
                Method::read(stream).expect("barrier").check(1, 50, 30).expect("AMQP method route");
                Method::write(stream, 1, 50, 31, &[0; 4]).expect("barrier reply");
                stream
                    .write_all(&returned_body(1, chunks).expect("AMQP fixture"))
                    .expect("returned body");
                if index == 0 {
                    Method::read(stream).expect("processed return").check(1, 50, 30)
                        .expect("barrier method");
                    Method::write(stream, 1, 50, 31, &[0; 4]).expect("processed reply");
                }
            }
            Method::read(stream).expect("close").check(0, 10, 50).expect("AMQP method route");
            Method::write(stream, 0, 10, 51, &[]).expect("close reply");
        },
        |broker| {
            let cap = config::Capacity::default()
                .channels(1)
                .channel(config::Channel::new(1, 0, 0, 0, 2))
                .body(config::Body::new(10, 16, 2));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);
            with(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("ready");
                    let chan = conn.channels().open(1).await.expect("channel");
                    {
                        let mut first = Pinned::new(chan.next_return());
                        assert!(poll_once(first.as_pin()).await.is_pending());
                        chan.queue().purge("first").await.expect("first barrier");
                        chan.queue().purge("processed").await.expect("return processed before cancellation");
                    }
                    let first = chan.next_return().await.expect("replacement receiver").expect("first body");
                    assert_eq!(first.body().as_slice(), Some(b"abcd".as_slice()));
                    assert_eq!(first.body().chunks().collect::<Vec<_>>(), [b"abcd".as_slice()]);
                    let mut second = Pinned::new(chan.next_return());
                    assert!(poll_once(second.as_pin()).await.is_pending());
                    chan.queue().purge("second").await.expect("second barrier");
                    let second = second.await.expect("second receive").expect("second body");
                    assert_eq!(second.body().len(), 6);
                    assert_eq!(second.body().as_slice(), None);
                    assert_eq!(second.body().chunks().collect::<Vec<_>>(), [b"ef".as_slice(), b"gh".as_slice(), b"ij".as_slice()]);
                    assert_eq!(first.body().as_slice(), Some(b"abcd".as_slice()));
                    let ((), count) = Tracker::usage(|| { second.release(); first.release(); });
                    assert_eq!(count, (0, 0));
                    let mut third = Pinned::new(chan.next_return());
                    assert!(poll_once(third.as_pin()).await.is_pending());
                    chan.queue().purge("reuse").await.expect("release barrier");
                    let third = third.await.expect("third receive").expect("reused storage");
                    assert_eq!(third.body().as_slice(), Some(b"kl".as_slice()));
                    third.release();
                    conn.control().close(ready, 200, "done").await.expect("close");
                })).expect("fiber");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("broker completion");
        },
    ).expect("broker");
}

#[test]
fn empty_needs_no_credit() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream)
                .expect("open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            Method::write(stream, 1, 20, 11, &[0; 4]).expect("open reply");
            Method::read(stream)
                .expect("barrier")
                .check(1, 50, 30)
                .expect("AMQP method route");
            Method::write(stream, 1, 50, 31, &[0; 4]).expect("barrier reply");
            stream
                .write_all(
                    &returned::frames(
                        1,
                        "",
                        "",
                        &[],
                        (1 << 12) | (1 << 11) | (1 << 6),
                        &[2, 9, 0, 0, 0, 0, 0, 0, 0, 42],
                    )
                    .expect("AMQP fixture"),
                )
                .expect("empty return");
            Method::read(stream)
                .expect("close")
                .check(0, 10, 50)
                .expect("AMQP method route");
            Method::write(stream, 0, 10, 51, &[]).expect("close reply");
        },
        |broker| {
            let cap = config::Capacity::default()
                .channels(1)
                .channel(config::Channel::new(1, 0, 0, 0, 1))
                .body(config::Body::new(0, 0, 0));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);
            with(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let chan = conn.channels().open(1).await.expect("channel");
                        let mut body = Pinned::new(chan.next_return());
                        assert!(poll_once(body.as_pin()).await.is_pending());
                        chan.queue().purge("empty").await.expect("barrier");
                        let body = body.await.expect("receive").expect("empty body");
                        assert!(body.body().is_empty());
                        assert_eq!(body.body().as_slice(), Some([].as_slice()));
                        assert_eq!(body.body().chunks().next(), None);
                        assert_eq!(body.properties().delivery(), Some(2));
                        assert_eq!(body.properties().priority(), Some(9));
                        assert_eq!(body.properties().timestamp, Some(42));
                        let ((), count) = Tracker::usage(|| body.release());
                        assert_eq!(count, (0, 0));
                        conn.control().close(ready, 200, "done").await.expect("close");
                    }))
                    .expect("fiber");
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
