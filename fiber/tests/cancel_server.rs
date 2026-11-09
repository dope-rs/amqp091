use std::time::Duration;

use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;

use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;
use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;
use crate::common::client::check_no_frame;

#[test]
fn is_terminal() {
    const TAG: &str = "server-root";

    AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first channel.open-ok");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let ok = consume_ok_arguments(TAG).expect("AMQP consumer tag");
            first
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("first basic.consume-ok");
            first
                .write_all(&frame(1, TAG).expect("AMQP fixture"))
                .expect("server basic.cancel");
            first
                .write_all(&delivery_frames(1, TAG).expect("AMQP fixture"))
                .expect("late basic.deliver");

            Method::read(&mut first)
                .expect("terminal consumer purge")
                .check(1, 50, 30)
                .expect("terminal consumer route");
            Method::write(&mut first, 1, 50, 31, &0u32.to_be_bytes())
                .expect("terminal consumer reply");
            check_no_frame(&first, "server cancel reply or duplicate client cancel")
                .expect("AMQP fixture");
            drop(first);

            let mut second = acceptor.accept().expect("recovered connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("recovered channel.open-ok");
            Method::read(&mut second)
                .expect("post-replay purge")
                .check(1, 50, 30)
                .expect("canceled consumer was not replayed");
            Method::write(&mut second, 1, 50, 31, &0u32.to_be_bytes())
                .expect("post-replay reply");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("replacement basic.consume-ok");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 60, 30)
                .expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"))
                .expect("replacement basic.cancel-ok");
            Method::write(&mut second, 0, 10, 50, &[0, 200, 0, 0, 0, 0, 0])
                .expect("retirement after replacement cancel-ok");
            Method::read(&mut second)
                .expect("connection.close-ok")
                .check(0, 10, 51)
                .expect("replacement cancellation processed");
        },
        move |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(1)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
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
                            let target = conn.observe().ready().await.expect("first ready");
                            let channel = conn
                                .channels().open_in(target, 1)
                                .await
                                .expect("first channel open");
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
                            let channel = channel.into_auto();
                            let canceled = consumer.recv().await.expect("server cancellation");
                            assert!(canceled.is_none());
                            drop(consumer);
                            channel.queue().purge("terminal").await.expect("terminal consumer reply");
                            conn.observe().retired(target.epoch()).await.expect("first retirement");
                            let recovered = conn.observe().ready().await.expect("topology replay");
                            channel.queue().purge("replayed").await.expect("no consumer replay");
                            let replacement = channel
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
                                .expect("replacement consumer capacity");
                            drop(replacement);
                            conn.observe().retired(recovered.epoch()).await.expect("replacement cancellation retired");
                        }))
                        .is_ok(),
                        "AMQP fiber must complete"
                    );
                },
                recover,
                complete,
            );
            broker.finish().expect("server cancellation broker");
        },
    )
    .expect("server cancellation broker");
}
