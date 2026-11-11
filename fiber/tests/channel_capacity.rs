use crate::common::amqp::app::{Endpoint, Policy};
use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use std::time::Duration;

use dope::manifold::timing;
use dope_amqp091::{adapter, config, failure};
use dope_fiber::task::local::Once;
use dope_test::alloc::Tracker;
use dope_test::harness::peer::TcpScript;
use failure::wire;

#[test]
fn rejects_and_reuses() {
    for (slots, server, client) in [(1, 16, 0), (2, 1, 0), (2, 16, 1)] {
        TcpScript::with(
            move |stream| {
                stream
                    .read_wait(Some(Duration::from_secs(10)))
                    .expect("read deadline");
                stream
                    .write_wait(Some(Duration::from_secs(10)))
                    .expect("write deadline");
                Peer::new(stream)
                    .open(Tune::new(server, 131_072, 0))
                    .expect("handshake");
                Method::read(stream)
                    .expect("first channel")
                    .check(1, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                    )
                    .expect("first open ok");
                Method::read(stream)
                    .expect("close after rejected admission")
                    .check(1, 20, 40).expect("AMQP method route");
                stream
                    .write_all(&Method::try_encode(1, 20, 41, &[]).expect("AMQP frame fixture"))
                    .expect("close ok");
                Method::read(stream)
                    .expect("reused channel")
                    .check(1, 20, 10).expect("AMQP method route");
                stream
                    .write_all(
                        &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                    )
                    .expect("reused open ok");
                Method::read(stream)
                    .expect("connection close")
                    .check(0, 10, 50).expect("AMQP method route");
                stream
                    .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                    .expect("connection close ok");
            },
            |broker| {
                let config = config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(config::Capacity::default().channels(slots))
                    .tune(config::Tune::default().channel_max(client));
                let policy = Policy::new(1, 16, Duration::from_secs(10));
                Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _, _>(policy, |rt| {
                let amqp = adapter::Adapter::new(rt.port());
                let conn = rt.connection();
                let ready = rt.block_on(conn.observe().ready()).expect("ready fiber").expect("ready");
                rt.block_on(Once::new(|mut access| {
                    let channels = amqp.channels();
                    let old = {
                        let slot = channels.reserve(access.reborrow(), ready, 1)
                            .expect("initial reservation");
                        slot.lease()
                    };
                    let slot = channels.reserve(access.reborrow(), ready, 1)
                        .expect("reuse after reservation drop");
                    assert!(slot.lease() != old, "reused channel generation");
                    channels.release(access, slot);
                })).expect("reservation lifecycle");
                let error = rt.block_on(conn.channels().open(0)).expect("reserved channel fiber").err().expect("reserved channel rejection");
                assert!(matches!(error.error(), failure::Error::Protocol(wire::Fault::ChannelZero)));
                let max = if client == 0 { server } else { server.min(client) };
                let error = rt.block_on(conn.channels().open(max + 1)).expect("channel bound fiber").err().expect("channel bound rejection");
                assert!(matches!(error.error(), failure::Error::Protocol(wire::Fault::ChannelMax { channel, max: actual }) if *channel == max + 1 && *actual == max));
                let first = rt.block_on(conn.channels().open_auto()).expect("first open fiber").expect("initial admission");
                assert_eq!(first.id(), 1);
                let duplicate = rt.block_on(conn.channels().open(1)).expect("duplicate open fiber").err().expect("duplicate channel opened");
                assert!(matches!(duplicate.error(), failure::Error::ChannelInUse(1)));
                let (result, counts) = Tracker::usage(|| rt.block_on(conn.channels().open_auto()));
                let error = result.expect("capacity fiber").err().expect("capacity rejection");
                assert!(matches!(error.error(), failure::Error::ChannelCapacity { capacity: 1 }));
                assert_eq!(counts, (0, 0));
                rt.block_on(first.close(200, "done")).expect("close fiber").expect("release channel");
                let second = rt.block_on(conn.channels().open_auto()).expect("reuse fiber").expect("admission after release");
                assert_eq!(second.id(), 1);
                rt.block_on(conn.control().close(ready, 200, "done")).expect("connection close fiber").expect("close connection");
                }, recover, complete);
                broker.finish().expect("broker completion");
            },
        ).expect("broker");
    }
}

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();
