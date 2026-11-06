use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};

pub mod common;
use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn isolates_equal_keys() {
    const LEFT: &str = "route-left";
    const RIGHT: &str = "route-right";

    TcpScript::with(
        move |stream| {
            Peer::new(stream)
                .open(Tune::new(2, 4096, 0))
                .expect("AMQP broker handshake");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("left channel.open-ok");
            Method::read(stream)
                .expect("AMQP method")
                .check(2, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("right channel.open-ok");

            check_client_consume(&Method::read(stream).expect("AMQP method"), LEFT)
                .expect("AMQP consumer method");
            let left_ok = consume_ok_arguments(LEFT).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &left_ok).expect("AMQP frame fixture"))
                .expect("left consume-ok");
            check_client_consume_on(&Method::read(stream).expect("AMQP method"), 2, RIGHT)
                .expect("AMQP consumer method");
            let right_ok = consume_ok_arguments(RIGHT).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(2, 60, 21, &right_ok).expect("AMQP frame fixture"))
                .expect("right consume-ok");

            check_on(&Method::read(stream).expect("AMQP method"), 1, LEFT)
                .expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &left_ok).expect("AMQP frame fixture"))
                .expect("left cancel-ok");
            stream
                .write_all(&delivery_frames(2, RIGHT).expect("AMQP fixture"))
                .expect("right consumer survives left retirement");

            check_on(&Method::read(stream).expect("AMQP method"), 2, RIGHT)
                .expect("AMQP cancel method");
            stream
                .write_all(&Method::try_encode(2, 60, 31, &right_ok).expect("AMQP frame fixture"))
                .expect("right cancel-ok");
            Method::write(stream, 0, 10, 50, &[0, 200, 0, 0, 0, 0, 0])
                .expect("retirement after right cancel-ok");
            Method::read(stream)
                .expect("connection.close-ok")
                .check(0, 10, 51)
                .expect("both cancellations processed");
        },
        move |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(2)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(
                    rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("ready");
                        let left_channel = conn.channels().open(1).await.expect("left channel");
                        let right_channel = conn.channels().open(2).await.expect("right channel");
                        let args = Table::new();
                        let left = left_channel
                            .basic()
                            .consume(basic::Consume {
                                queue: "q",
                                consumer_tag: LEFT,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("left consumer");
                        let mut right = right_channel
                            .basic()
                            .consume(basic::Consume {
                                queue: "q",
                                consumer_tag: RIGHT,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("right consumer");

                        drop(left);
                        let delivery = right.recv().await.expect("right consumer active")
                            .expect("right delivery after left retirement");
                        assert_eq!(delivery.consumer_tag(), RIGHT);
                        assert!(delivery.body() == b"x"[..]);
                        delivery.release();
                        drop(right);
                        conn.observe().retired(target.epoch()).await.expect("both cancellations retired");
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
            broker.finish().expect("broker completion");
        },
    )
    .expect("broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
