use std::convert::Infallible;
use std::io::Write as _;

use dope::core::driver::life::recovery;
use dope_amqp091::config;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;

pub mod common;

use dope_amqp091::failure;
use dope_amqp091::protocol::wire::frame;
use dope_test::harness::peer::TcpScript;

use crate::common::client::*;

#[test]
fn rejects_large_header() {
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
            let mut header = vec![1, 0, 0];
            header.extend_from_slice(&131_072u32.to_be_bytes());
            stream.write_all(&header).expect("oversized frame header");
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
                            wait_ready(conn).await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("open channel");
                            let result = channel.queue().purge("trigger").await;
                            std::assert_matches!(result,
                                Err(error) if matches!((error).error(), failure::Error::Frame(
                                    frame::Fault::FrameTooLarge {
                                    size: 131_080,
                                    max: 131_072,
                                    }
                                ))
                            );
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("broker");
        },
    )
    .expect("spawn oversized-frame broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
