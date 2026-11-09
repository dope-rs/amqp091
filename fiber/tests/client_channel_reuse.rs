use dope_amqp091::config;
use dope_amqp091::message::property::Properties;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_amqp091::failure;
use dope_test::harness::peer::TcpScript;

use crate::common::client::{channel, *};

#[test]
fn reuses_id() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first channel open ok");
            Method::read(stream).expect("AMQP method").check(1, 20, 40).expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(1, 20, 41, &[]).expect("AMQP frame fixture"))
                .expect("channel close ok");
            Method::read(stream).expect("AMQP method").check(1, 20, 10).expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("second channel open ok");
            let fresh = Method::read(stream).expect("AMQP method");
            fresh.check(1, 50, 30).expect("AMQP method route");
            assert_eq!(queue_name(&fresh).expect("AMQP fixture"), "fresh");
            stream
                .write_all(
                    &Method::try_encode(1, 50, 31, &0u32.to_be_bytes())
                        .expect("AMQP frame fixture"),
                )
                .expect("fresh purge ok");
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
                    .capacity(config::Capacity::default().channels(1)),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let first = conn.channels().open_auto().await.expect("first auto channel");
                        assert_eq!(first.id(), 1);
                        let late = first.queue().purge("late");
                        let mut close = Pinned::new(first.close(200, "OK"));
                        assert!(poll_once(close.as_pin()).await.is_pending());
                        let late = late.await;
                        assert!(matches!(late, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        close.await.expect("close channel");
                        let second = conn.channels().open_auto().await.expect("second auto channel");
                        assert_eq!(second.id(), 1);
                        let properties = Properties::default();
                        let (publish, allocation) = Tracker::usage(|| {
                            first
                                .basic()
                                .publish(channel::request("", "stale", &properties, b"old"))
                        });
                        assert_eq!(allocation, (0, 0));
                        let publish = publish.await;
                        assert!(matches!(publish, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        let stale_purge = first.queue().purge("stale").await;
                        assert!(matches!(stale_purge, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        let stale_close = first.close(200, "stale").await;
                        assert!(matches!(stale_close, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        let stale_flow = first.flow().await;
                        assert!(matches!(stale_flow, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        let stale_return = first.next_return().await;
                        assert!(matches!(stale_return, Err(ref error) if matches!(error.error(), failure::Error::Closed)));
                        second.queue().purge("fresh").await.expect("fresh purge");
                        conn.control()
                            .close(ready, 200, "OK")
                            .await
                            .expect("close");
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                complete,
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn auto-channel broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();
