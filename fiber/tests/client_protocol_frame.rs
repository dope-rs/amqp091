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
fn rejects_invalid_frames() {
    for zero_tag in [false, true] {
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
            let frame = if zero_tag {
                common::basic::delivery::method(1, "consumer", 0)
                    .expect("zero delivery tag fixture")
            } else {
                let mut header = vec![1, 0, 0];
                header.extend_from_slice(&131_072u32.to_be_bytes());
                header
            };
            stream.write_all(&frame).expect("invalid frame");
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
                            let error = channel.queue().purge("trigger").await
                                .expect_err("invalid frame must fail the pending request");
                            assert!(matches!((zero_tag, error.error()),
                                (false, failure::Error::Frame(
                                    frame::Fault::FrameTooLarge {
                                    size: 131_080,
                                    max: 131_072,
                                    }
                                ))
                                | (true, failure::Error::Protocol(failure::wire::Fault::DeliveryTagZero))
                            ), "{:?}", error.error());
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
    .expect("spawn invalid-frame broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
