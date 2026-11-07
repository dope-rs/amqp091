use std::time::Duration;

use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Peer, Tune};
use crate::common::fiber::{Pinned, poll_once};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;

#[test]
fn drops_uncommitted() {
    const TAG: &str = "transport-intent";

    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(Tune::new(2, 4096, 0))
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
                .check(1, 60, 10)
                .expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 60, 11, &[]).expect("AMQP frame fixture"))
                .expect("first basic.qos-ok");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            Method::read(&mut first)
                .expect("request observation channel")
                .check(2, 20, 10)
                .expect("request observation route");
            Method::write(&mut first, 2, 20, 11, &[0, 0, 0, 0])
                .expect("request observation reply");
            Method::read(&mut first)
                .expect("future drop close")
                .check(2, 20, 40)
                .expect("future drop route");
            Method::write(&mut first, 2, 20, 41, &[]).expect("future drop reply");
            drop(first);

            let mut second = broker.accept().expect("recovered connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(Tune::new(2, 4096, 0))
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
                .expect("AMQP method")
                .check(1, 60, 10)
                .expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 60, 11, &[]).expect("AMQP frame fixture"))
                .expect("recovered basic.qos-ok");
            Method::read(&mut second)
                .expect("post-replay purge")
                .check(1, 50, 30)
                .expect("uncommitted consumer was not replayed");
            Method::write(&mut second, 1, 50, 31, &0u32.to_be_bytes())
                .expect("post-replay reply");

            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            let ok = consume_ok_arguments(TAG).expect("AMQP consumer tag");
            second
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("replacement consume-ok");
            check(&Method::read(&mut second).expect("AMQP method"), TAG)
                .expect("AMQP cancel method");
            second
                .write_all(&Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"))
                .expect("replacement cancel-ok");
            Method::write(&mut second, 0, 10, 50, &[0, 200, 0, 0, 0, 0, 0])
                .expect("retirement after replacement cancel-ok");
            Method::read(&mut second)
                .expect("connection.close-ok")
                .check(0, 10, 51)
                .expect("replacement cancellation processed");
        },
        move |broker| {
            let addr = broker.addr();

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
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                addr,
                config,
                |rt| {
                    let conn = rt.connection();
                    let run = dope_fiber::fiber!('_ => async move {
                        let target = conn
                            .observe().ready()
                            .await
                            .expect("first ready");
                        let chan = conn
                            .channels().open_in(target, 1)
                            .await
                            .expect("first channel open");
                        chan
                            .basic()
                            .qos(1)
                            .await
                            .expect("topology anchor qos");
                        let chan = chan.into_auto();
                        let args = Table::new();
                        let mut op = Pinned::new(chan.basic().consume(basic::Consume {
                            queue: "q",
                            consumer_tag: TAG,
                            no_local: false,
                            ack: ack::Manual,
                            exclusive: false,
                            arguments: &args,
                        }));
                        assert!(poll_once(op.as_pin()).await.is_pending());
                        let barrier = conn.channels().open(2).await.expect("consume request observed");
                        drop(op);
                        barrier.close(200, "").await.expect("future drop observed");
                        conn.observe().retired(target.epoch()).await.expect("first retirement");
                        let recovered = conn.observe().ready().await.expect("topology replay");
                        chan.queue().purge("replayed").await.expect("no consumer replay");
                        let next = chan
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
                            .expect("replacement consumer");
                        drop(next);
                        conn.observe().retired(recovered.epoch()).await.expect("replacement cancellation retired");
                    });
                    assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("transport intent broker");
        },
    )
    .expect("transport intent broker");
}
