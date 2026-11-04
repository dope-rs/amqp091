use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::app::{Endpoint, plan};
use crate::common::amqp::wire::{Frame, Method};
pub mod common;

use dope::manifold::timing;
use dope_amqp091::message::property::Properties;
use dope_amqp091::{adapter, config, failure};
use dope_test::harness::peer::TcpScript;

use crate::common::basic::returned;
use crate::common::{basic, client};

#[test]
fn incomplete_closes_505() {
    const ROUTE: &str = "official.incomplete";
    const PUBLISHED: &[u8] = b"xy";

    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            let channel = returned::open_channel(stream).expect("AMQP fixture");
            returned::check_publish(stream, channel, basic::EXCHANGE, ROUTE, PUBLISHED, None)
                .expect("AMQP fixture");
            stream
                .write_all(
                    &returned::method(channel, basic::EXCHANGE, ROUTE).expect("AMQP fixture"),
                )
                .expect("basic.return");
            stream
                .write_all(
                    &basic::content_header(channel, PUBLISHED.len() as u64).expect("AMQP fixture"),
                )
                .expect("return content header");
            stream
                .write_all(&Frame::try_encode(3, channel, b"x").expect("AMQP frame fixture"))
                .expect("partial return body");
            stream
                .write_all(&Method::try_encode(channel, 20, 20, &[1]).expect("AMQP frame fixture"))
                .expect("method interrupting return content");
            returned::check_connection_close(stream, 505).expect("AMQP fixture");
        },
        |broker| {
            Endpoint::new(broker.addr(), plan::returns().expect("AMQP configuration"))
                .try_check::<timing::Throughput, _>(
                basic::POLICY,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel open");
                    let props = Properties::default();
                    channel
                        .basic()
                        .publish(adapter::publish::Request {
                            exchange: basic::EXCHANGE,
                            routing_key: ROUTE,
                            mandatory: true,
                            properties: &props,
                            body: PUBLISHED,
                        })
                        .await
                        .expect("mandatory publish");
                    let error = channel.next_return().await.err().expect("interrupted content");
                    assert!(matches!(error.error(), failure::Error::Protocol(
                        failure::wire::Fault::ContentInterrupted
                    )));
                    conn.observe().retired(ready.epoch()).await.expect("protocol close retirement");
                })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

#[test]
fn wrong_class_closes_502() {
    const ROUTE: &str = "official.wrong-class";

    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            let channel = returned::open_channel(stream).expect("AMQP fixture");
            returned::check_publish(stream, channel, basic::EXCHANGE, ROUTE, &[], None)
                .expect("AMQP fixture");
            stream
                .write_all(
                    &returned::method(channel, basic::EXCHANGE, ROUTE).expect("AMQP fixture"),
                )
                .expect("basic.return");
            let mut header = Vec::with_capacity(14);
            header.extend_from_slice(&50_u16.to_be_bytes());
            header.extend_from_slice(&0_u16.to_be_bytes());
            header.extend_from_slice(&0_u64.to_be_bytes());
            header.extend_from_slice(&0_u16.to_be_bytes());
            stream
                .write_all(&Frame::try_encode(2, channel, &header).expect("AMQP frame fixture"))
                .expect("non-basic content header");
            returned::check_connection_close(stream, 502).expect("AMQP fixture");
        },
        |broker| {
            Endpoint::new(broker.addr(), plan::returns().expect("AMQP configuration"))
                .try_check::<timing::Throughput, _>(
                basic::POLICY,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                    let ready = conn.observe().ready().await.expect("ready");
                    let channel = conn.channels().open(1).await.expect("channel open");
                    let props = Properties::default();
                    channel
                        .basic()
                        .publish(adapter::publish::Request {
                            exchange: basic::EXCHANGE,
                            routing_key: ROUTE,
                            mandatory: true,
                            properties: &props,
                            body: &[],
                        })
                        .await
                        .expect("mandatory publish");
                    let error = channel.next_return().await.err().expect("non-basic content class");
                    assert!(matches!(error.error(), failure::Error::Protocol(
                        failure::wire::Fault::ContentClass { actual: 50 }
                    )));
                    conn.observe().retired(ready.epoch()).await.expect("protocol close retirement");
                })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}

#[test]
fn duplicate_before_retry() {
    const ROUTE: &str = "official.duplicate-header";

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(basic::TUNE).expect("AMQP broker handshake");
            let channel = returned::open_channel(stream).expect("AMQP fixture");
            returned::check_publish(stream, channel, basic::EXCHANGE, ROUTE, b"x", None)
                .expect("AMQP fixture");
            stream.write_all(&returned::method(channel, basic::EXCHANGE, ROUTE).expect("AMQP fixture"))
                .expect("basic.return");
            let header = basic::content_header(channel, 1).expect("AMQP fixture");
            stream.write_all(&header).expect("first content header");
            stream.write_all(&header).expect("duplicate content header");
            returned::check_connection_close(stream, 502).expect("AMQP fixture");
        },
        |broker| {
            let config = config::Config::new("guest", "guest").expect("AMQP configuration").capacity(
                config::Capacity::default()
                    .channels(1)
                    .channel(config::Channel::default().methods(1).returns(1))
                    .body(config::Body::default().buffer(1))
                    .egress(config::Egress::new(4, 4096)),
            );
            client::with(
                broker.addr(),
                config,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel open");
                        let props = Properties::default();
                        channel.basic().publish(adapter::publish::Request {
                            exchange: basic::EXCHANGE,
                            routing_key: ROUTE,
                            mandatory: true,
                            properties: &props,
                            body: b"x",
                        }).await.expect("mandatory publish");
                        let error = channel.next_return().await.err().expect("duplicate header");
                        assert!(matches!(error.error(), failure::Error::Protocol(
                            failure::wire::Fault::ContentHeaderDuplicate { properties: true, body: true }
                        )));
                        conn.observe().retired(ready.epoch()).await.expect("protocol close retirement");
                    })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    ).expect("scripted AMQP broker");
}

#[test]
fn unbound_body_closes_502() {
    const ROUTE: &str = "official.unbound-body";

    TcpScript::with(
        |stream| {
            Peer::new(stream).open(basic::TUNE).expect("AMQP broker handshake");
            let channel = returned::open_channel(stream).expect("AMQP fixture");
            returned::check_publish(stream, channel, basic::EXCHANGE, ROUTE, b"x", None)
                .expect("AMQP fixture");
            stream.write_all(&returned::method(channel, basic::EXCHANGE, ROUTE).expect("AMQP fixture"))
                .expect("basic.return");
            stream.write_all(&Frame::try_encode(3, channel, b"x").expect("AMQP frame fixture"))
                .expect("body before content header");
            returned::check_connection_close(stream, 502).expect("AMQP fixture");
        },
        |broker| {
            client::with(
                broker.addr(),
                plan::returns().expect("AMQP configuration"),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let ready = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open(1).await.expect("channel open");
                        let props = Properties::default();
                        channel.basic().publish(adapter::publish::Request {
                            exchange: basic::EXCHANGE,
                            routing_key: ROUTE,
                            mandatory: true,
                            properties: &props,
                            body: b"x",
                        }).await.expect("mandatory publish");
                        let error = channel.next_return().await.err().expect("body before header");
                        assert!(matches!(error.error(), failure::Error::Protocol(
                            failure::wire::Fault::ContentBodyUnbound { actual: 1 }
                        )));
                        conn.observe().retired(ready.epoch()).await.expect("protocol close retirement");
                    })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    ).expect("scripted AMQP broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
