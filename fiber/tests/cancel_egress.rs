use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::ack::Disposition;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;

pub mod common;

use dope_test::harness::peer::TcpScript;

use crate::common::cancel::*;

#[test]
fn cancels_when_full() {
    const TAG: &str = "drop-root";

    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");

            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");

            check_client_consume(&Method::read(stream).expect("AMQP method"), TAG)
                .expect("AMQP consumer method");
            let ok = consume_ok_arguments(TAG).expect("AMQP consumer tag");
            stream
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("basic.consume-ok");
            stream
                .write_all(&delivery_frames_with_tag(1, TAG, 1).expect("AMQP fixture"))
                .expect("first basic.deliver");
            stream
                .write_all(&delivery_frames_with_tag(1, TAG, 2).expect("AMQP fixture"))
                .expect("second basic.deliver");

            for _ in 0..2 {
                let ack = Method::read(stream).expect("AMQP method");
                ack.check(1, 60, 80).expect("AMQP method route");
            }

            check(&Method::read(stream).expect("AMQP method"), TAG).expect("AMQP cancel method");
            stream
                .write_all(&frame(1, TAG).expect("AMQP fixture"))
                .expect("racing server basic.cancel");
            stream
                .write_all(&Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"))
                .expect("basic.cancel-ok");
            stream
                .write_all(
                    &Method::try_encode(0, 10, 50, &[0, 200, 0, 0, 0, 0, 0])
                        .expect("AMQP frame fixture"),
                )
                .expect("retirement after cancel race");
            Method::read(stream)
                .expect("connection.close-ok")
                .check(0, 10, 51)
                .expect("cancel race processed before retirement");
        },
        |broker| {
            let addr = broker.addr();

            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(2)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                addr,
                config,
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open(1).await.expect("channel open");
                            let args = Table::new();
                            let mut consumer = channel
                                .basic()
                                .consume(basic::Consume {
                                    queue: "q",
                                    consumer_tag: TAG,
                                    no_local: false,
                                    ack: ack::Manual,
                                    exclusive: false,
                                    arguments: &args,
                                })
                                .await
                                .expect("basic.consume");

                            let first = consumer
                                .recv()
                                .await
                                .expect("consumer active")
                                .expect("first delivery");
                            let second = consumer
                                .recv()
                                .await
                                .expect("consumer active")
                                .expect("second delivery");
                            let ((first, second), alloc) = Tracker::usage(|| {
                                let first = first.settle(Disposition::Ack);
                                let second = second.settle(Disposition::Ack);
                                drop(consumer);
                                (first, second)
                            });
                            assert_eq!(alloc, (0, 0));
                            first.await.expect("fill egress");
                            second.await.expect("fill egress");

                            conn.observe().retired(target.epoch()).await.expect("cancel race retirement");
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

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
