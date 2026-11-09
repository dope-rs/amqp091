use dope_amqp091::{
    config,
    message::{field::view, property::Properties},
};
use dope_fiber::abi::Join;
use dope_test::harness::peer::TcpScript;

use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;
use common::{
    amqp::{Peer, wire::Method},
    client::{
        STAGING_BOUNDARY, STAGING_HELD, STAGING_NEXT, TUNE, channel::request, check_publish,
        returned_body, staging_header, with,
    },
    fiber::{Pinned, poll_once},
};

#[test]
fn retains_multiple_staged_frames_until_one_message_completes() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE)?;
            let channel = common::basic::returned::open_channel(stream)?;
            // Each body frame exceeds Throughput's 64 KiB receive buffer, so
            // both must stage even when the transport coalesces every write.
            let frames = returned_body(channel, &[&STAGING_HELD, &STAGING_NEXT])?;
            stream.write_all(&frames).map_err(common::amqp::Error::Io)?;
            common::basic::returned::close_connection(stream)
        },
        |broker| {
            let capacity = config::Capacity::new(
                1,
                config::Channel::new(1, 0, 0, 0, 1),
                config::Body::default().buffer(256 * 1024),
                config::Egress::new(4, 4096),
            );
            let config = config::Config::new("guest", "guest")
                .expect("fixture identity")
                .capacity(capacity);
            with(
                broker.addr(),
                config,
                |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let message = match channel.next_return().await {
                            Ok(message) => message.expect("complete message"),
                            Err(error) => panic!("message operation: {:?}", error.error()),
                        };
                        assert!(message.body().chunks().eq([&STAGING_HELD[..], &STAGING_NEXT[..]]));
                        message.release();
                        conn.control().close(ready, 200, "OK").await.expect("close");
                    }))
                    .expect("fiber");
                },
                recover,
                complete,
            );
            broker
                .finish()
                .expect("broker completion")
                .expect("broker I/O");
        },
    )
    .expect("scripted broker");
}

#[test]
fn reclaims_retained_and_consumed_staging_between_fragmented_frames() {
    for (first, second, release_early) in [(98304, 98304, false), (58566, 93818, true)] {
        let first = &STAGING_HELD[..first];
        let second = &STAGING_NEXT[..second];
        TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("handshake");
            Method::read(stream).expect("open").check(1, 20, 10).expect("open route");
            Method::write(stream, 1, 20, 11, &[0, 0, 0, 0]).expect("open ok");
            let chunks = [first];
            let initial = returned_body(1, &chunks).expect("held frames");
            let cut = initial.len() - first.len();
            stream.write_all(&initial[..cut]).expect("held prefix");
            Method::read(stream).expect("first barrier").check(1, 50, 30).expect("first route");
            stream.write_all(&initial[cut..]).expect("held suffix");
            Method::write(stream, 1, 50, 31, &0u32.to_be_bytes()).expect("first ok");
            Method::read(stream).expect("next barrier").check(1, 50, 30).expect("barrier route");
            let frames = returned_body(1, &[second]).expect("next frames");
            // Leave a complete body-frame header and one body byte in ingress.
            // Exercise a retained backing and an already-consumed smaller
            // backing that must be reclaimed before a larger reservation.
            let split = frames.len() - second.len();
            let mut prefix = Method::try_encode(1, 50, 31, &0u32.to_be_bytes()).expect("barrier ok");
            prefix.extend_from_slice(&frames[..split]);
            stream.write_all(&prefix).expect("fragmented prefix");
            Method::read(stream).expect("release barrier").check(1, 50, 30).expect("release route");
            stream.write_all(&frames[split..]).expect("fragmented suffix");
            Method::write(stream, 1, 50, 31, &0u32.to_be_bytes()).expect("release ok");
            Method::read(stream).expect("close").check(0, 10, 50).expect("close route");
            Method::write(stream, 0, 10, 51, &[]).expect("close ok");
        },
        move |broker| {
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(config::Channel::new(1, 0, 1, 4, 2))
                .body(config::Body::default().buffer(first.len() + second.len()).retention(0, usize::MAX));
            let config = config::Config::new("guest", "guest").expect("fixture identity").capacity(capacity);
            with(broker.addr(), config, |rt| {
                let conn = rt.connection();
                assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel");
                    let mut held = Pinned::new(channel.next_return());
                    assert!(poll_once(held.as_pin()).await.is_pending());
                    let (first_barrier, held) = Join::new(channel.queue().purge("first"), held).await;
                    first_barrier.expect("first barrier");
                    let held = held.expect("held operation").expect("held return");
                    assert!(held.body() == first[..]);
                    let mut held = Some(held);
                    if release_early { held.take().expect("held body").release(); }
                    let mut next = Pinned::new(channel.next_return());
                    assert!(poll_once(next.as_pin()).await.is_pending());
                    channel.queue().purge("next").await.expect("prefix barrier");
                    assert!(poll_once(next.as_pin()).await.is_pending(), "staging pressure must preserve the generation");
                    if let Some(held) = held { held.release(); }
                    let (released, next) = Join::new(channel.queue().purge("release"), next).await;
                    released.expect("release barrier");
                    let next = next.expect("resumed operation").expect("resumed return");
                    assert!(next.body() == second[..]);
                    next.release();
                    conn.control().close(ready, 200, "OK").await.expect("close");
                })).is_ok());
            }, recover, complete);
            broker.finish().expect("broker completion");
        },
    ).expect("scripted broker");
    }
}

#[test]
fn reclaims_retained_and_consumed_staging_for_fragmented_header() {
    for release_early in [false, true] {
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE)?;
                let channel = common::basic::returned::open_channel(stream)?;
                stream
                    .write_all(&returned_body(channel, &[&STAGING_BOUNDARY])?)
                    ?;
                Method::read(stream)?.check(channel, 50, 30)?;
                let header = staging_header(channel)?;
                let mut prefix = Method::try_encode(channel, 50, 31, &[0; 4])?;
                prefix.extend_from_slice(&common::basic::returned::method(channel, "", "")?);
                prefix.extend_from_slice(&header[..3]);
                stream.write_all(&prefix)?;
                stream.write_all(&header[3..7])?;
                check_publish(stream, channel, "header", &[])?;
                stream.write_all(&header[7..])?;
                common::basic::returned::close_connection(stream)
            },
            |broker| {
                // The first wire backing is body + 8; the next header is body + 22.
                // Sharing needs those 8 extra resident bytes. With a copied and
                // released first body, the smaller budget admits the next property
                // exactly and exposes unnecessary old/new staging overlap.
                let bytes = STAGING_BOUNDARY.len() + if release_early { 0 } else { 8 };
                let retention = if release_early { (usize::MAX, 0) } else { (0, usize::MAX) };
                let capacity = config::Capacity::default()
                    .channels(1)
                    .channel(config::Channel::new(1, 0, 1, 4, 2))
                    .body(config::Body::default().buffer(bytes).retention(retention.0, retention.1));
                let config = config::Config::new("guest", "guest").expect("identity").capacity(capacity);
                with(broker.addr(), config, |rt| {
                    let conn = rt.connection();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let held = channel.next_return().await.expect("first operation").expect("first return");
                        assert!(held.body() == STAGING_BOUNDARY[..]);
                        let mut held = Some(held);
                        if release_early { held.take().expect("first owner").release(); }
                        let mut next = Pinned::new(channel.next_return());
                        assert!(poll_once(next.as_pin()).await.is_pending());
                        channel.queue().purge("prefix").await.expect("prefix reply");
                        // Unconfirmed publication needs no reply behind the partial header.
                        channel.basic().publish(request("", "header", &Properties::default(), &[]))
                            .await.expect("header publication");
                        if let Some(held) = held {
                            assert!(poll_once(next.as_pin()).await.is_pending());
                            held.release();
                        }
                        let next = next.await.expect("header operation").expect("header return");
                        assert!(next.body().is_empty());
                        let properties = next.properties();
                        let mut headers = properties.headers().expect("headers").iter();
                        let Some(("x", view::Value::String(value))) = headers.next() else { panic!("header value") };
                        assert_eq!(value.len(), STAGING_BOUNDARY.len() - 11);
                        assert!(value.iter().all(|byte| *byte == b'x'));
                        assert!(headers.next().is_none());
                        next.release();
                        conn.control().close(ready, 200, "OK").await.expect("close");
                    })).expect("fiber");
                }, recover, complete);
                broker
                    .finish()
                    .expect("broker completion")
                    .unwrap_or_else(|error| panic!("broker I/O, release_early={release_early}: {error:?}"));
            },
        ).expect("scripted broker");
    }
}
