use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::platform::wake;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::wait;

pub mod common;
use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;

#[test]
fn replays_without_identity() {
    let (observed, notify) = wake::Ends::blocking().expect("broker observations").split();
    let (ready, release) = wake::Ends::blocking().expect("consumer barrier").split();
    let tag = std::str::from_utf8(&[b't'; 255]).expect("ASCII consumer tag");

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
            check_client_consume(&Method::read(&mut first).expect("AMQP method"), tag)
                .expect("AMQP consumer method");
            let ok = consume_ok_arguments(tag).expect("AMQP consumer tag");
            first
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("first basic.consume-ok");
            ready
                .wait_timeout(Duration::from_secs(10))
                .expect("consumer ready");
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
            check_client_consume(&Method::read(&mut second).expect("AMQP method"), tag)
                .expect("AMQP consumer method");
            second
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("replayed basic.consume-ok");
            second
                .write_all(&delivery_frames(1, tag).expect("AMQP fixture"))
                .expect("replayed delivery");
            check(&Method::read(&mut second).expect("AMQP method"), tag)
                .expect("AMQP cancel method");
            second
                .write_all(&Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"))
                .expect("replayed basic.cancel-ok");
            notify.notify().expect("replayed cancel-ok written");
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
                .topology(config::Topology::default().identities(0))
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
                                consumer_tag: tag,
                                no_local: false,
                                ack: ack::Manual,
                                exclusive: false,
                                arguments: &args,
                            })
                            .await
                            .expect("first basic.consume");
                        let tracked = conn
                            .observe()
                            .read(|view| view.channels())
                            .await;
                        assert_eq!(
                            tracked,
                            1,
                            "a consumer without a topology identity must still retain its channel",
                        );
                        release.notify().expect("consumer ready");
                        let delivery = consumer
                            .recv()
                            .await
                            .expect("replayed receive")
                            .expect("replayed delivery");
                        assert_eq!(delivery.consumer_tag(), tag);
                        delivery.release();
                        drop(consumer);
                        wait(&observed).await.expect("replayed cancel-ok written");
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
            broker.finish().expect("identity replay broker");
        },
    )
    .expect("identity replay broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
