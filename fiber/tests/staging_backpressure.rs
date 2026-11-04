use std::{convert::Infallible, time::Duration};

use dope::core::driver::life::recovery;
use dope::core::platform::wake;
use dope_amqp091::{config, message::field::view};
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::harness::peer::TcpScript;

pub mod common;
use common::{
    amqp::{Peer, wire::Method},
    client::{
        STAGING_BOUNDARY, STAGING_HELD, STAGING_NEXT, TUNE, returned_body, staging_header,
        wait_ready, with,
    },
    fiber::{Pinned, poll_once, wait},
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
                        let ready = wait_ready(conn).await.expect("ready");
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
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
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
                let timer = rt.timer();
                assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = wait_ready(conn).await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel");
                    let mut held = Pinned::new(channel.next_return());
                    assert!(poll_once(held.as_pin()).await.is_pending());
                    timer.sleep(Duration::from_millis(50)).expect("first staging turn").await;
                    let mut first_barrier = Pinned::new(channel.queue().purge("first"));
                    assert!(poll_once(first_barrier.as_pin()).await.is_pending());
                    let held = held.await.expect("held operation").expect("held return");
                    first_barrier.await.expect("first barrier");
                    assert!(held.body() == first[..]);
                    let mut held = Some(held);
                    if release_early { held.take().expect("held body").release(); }
                    let mut next = Pinned::new(channel.next_return());
                    assert!(poll_once(next.as_pin()).await.is_pending());
                    channel.queue().purge("next").await.expect("prefix barrier");
                    timer.sleep(Duration::from_millis(50)).expect("staging turn").await;
                    assert!(poll_once(next.as_pin()).await.is_pending(), "staging pressure must preserve the generation");
                    if let Some(held) = held { held.release(); }
                    let mut released = Pinned::new(channel.queue().purge("release"));
                    assert!(poll_once(released.as_pin()).await.is_pending());
                    let next = next.await.expect("resumed operation").expect("resumed return");
                    assert!(next.body() == second[..]);
                    next.release();
                    released.await.expect("release barrier");
                    conn.control().close(ready, 200, "OK").await.expect("close");
                })).is_ok());
            }, recover, |result| {
                result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
            });
            broker.finish().expect("broker completion");
        },
    ).expect("scripted broker");
    }
}

#[test]
fn fragmented_header_resumes_on_release_and_reclaims_consumed_backing() {
    for release_early in [false, true] {
        let (payload, proceed) = wake::Ends::blocking().expect("payload gate").split();
        let (written, notify) = wake::Ends::blocking().expect("wire completion").split();
        TcpScript::with(
            move |stream| {
                use common::amqp::Error;

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
                Method::read(stream)?.check(channel, 50, 30)?;
                stream.write_all(&header[3..7])?;
                payload.wait_timeout(Duration::from_secs(10)).map_err(Error::Wait)?;
                stream.write_all(&header[7..])?;
                Method::write(stream, channel, 50, 31, &[0; 4])?;
                notify.notify().map_err(Error::Notify)?;
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
                    let timer = rt.timer();
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = wait_ready(conn).await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel");
                        let held = channel.next_return().await.expect("first operation").expect("first return");
                        assert!(held.body() == STAGING_BOUNDARY[..]);
                        let mut held = Some(held);
                        if release_early { held.take().expect("first owner").release(); }
                        let mut next = Pinned::new(channel.next_return());
                        assert!(poll_once(next.as_pin()).await.is_pending());
                        channel.queue().purge("prefix").await.expect("prefix reply");
                        timer.sleep(Duration::from_millis(50)).expect("three-byte staging turn").await;
                        let mut header = Pinned::new(channel.queue().purge("header"));
                        assert!(poll_once(header.as_pin()).await.is_pending());
                        timer.sleep(Duration::from_millis(50)).expect("seven-byte staging turn").await;
                        proceed.notify().expect("allow complete next frame");
                        // A broker failure closes this pipe. Preserve that result
                        // while observing the client's original terminal cause.
                        let written = wait(&written).await;
                        timer.sleep(Duration::from_millis(50)).expect("retained ingress turn").await;
                        if let Some(held) = held {
                            match poll_once(next.as_pin()).await {
                                std::task::Poll::Pending => {}
                                std::task::Poll::Ready(Err(error)) => panic!("retained header, release_early={release_early}, written={written:?}: {:?}", error.error()),
                                std::task::Poll::Ready(Ok(_)) => panic!("retained owner did not block header, release_early={release_early}"),
                            }
                            // All broker writes and client requests precede this
                            // release. No purge, heartbeat, or new input can supply
                            // the connection wake needed to retry this staged owner.
                            held.release();
                        }
                        let next = match next.await {
                            Ok(Some(next)) => next,
                            Ok(None) => panic!("missing header return, release_early={release_early}"),
                            Err(error) => panic!("fragmented header, release_early={release_early}, written={written:?}: {:?}", error.error()),
                        };
                        written.expect("complete next frame written");
                        assert!(next.body().is_empty());
                        let properties = next.properties();
                        let mut headers = properties.headers().expect("headers").iter();
                        let Some(("x", view::Value::String(value))) = headers.next() else { panic!("header value") };
                        assert_eq!(value.len(), STAGING_BOUNDARY.len() - 11);
                        assert!(value.iter().all(|byte| *byte == b'x'));
                        assert!(headers.next().is_none());
                        next.release();
                        header.await.expect("header request completion");
                        conn.control().close(ready, 200, "OK").await.expect("close");
                    })).expect("fiber");
                }, recover, |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                });
                broker
                    .finish()
                    .expect("broker completion")
                    .unwrap_or_else(|error| panic!("broker I/O, release_early={release_early}: {error:?}"));
            },
        ).expect("scripted broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
