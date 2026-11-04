use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_fiber::task::time::sleep::TimerExt as _;
use dope_test::alloc;
use dope_test::alloc::Tracker;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;

pub mod common;

use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;

#[test]
fn releases_lease() {
    let done = AtomicBool::new(false);
    const TAG: &str = "retire-root";

    let done = &done;
    let peer = done;
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
            check_client_consume(&Method::read(&mut first).expect("AMQP method"), TAG)
                .expect("AMQP consumer method");
            let ok = consume_ok_arguments(TAG).expect("AMQP consumer tag");
            first
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("first basic.consume-ok");
            check(&Method::read(&mut first).expect("AMQP method"), TAG)
                .expect("AMQP cancel method");
            drop(first);

            let mut second = broker.accept().expect("recovered connection");
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

            check_client_consume(&Method::read(&mut second).expect("AMQP method"), TAG)
                .expect("AMQP consumer method");
            second
                .write_all(&Method::try_encode(1, 60, 21, &ok).expect("AMQP frame fixture"))
                .expect("recovered basic.consume-ok");
            check(&Method::read(&mut second).expect("AMQP method"), TAG)
                .expect("AMQP cancel method");
            second
                .write_all(&Method::try_encode(1, 60, 31, &ok).expect("AMQP frame fixture"))
                .expect("recovered basic.cancel-ok");
            peer.store(true, Ordering::Release);
        },
        |broker| {
            let addr = broker.addr();

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
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with_client(
                addr,
                config,
                |rt| {
                    let conn = rt.connection();
                    let timer = rt.timer();
                    let run = dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let epoch = conn.observe().read(|view| view.generation()).await;
                        let channel = conn
                            .channels().open_in(target, 1)
                            .await
                            .expect("first channel open");
                        let args = Table::new();
                        let consumer = channel
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
                            .expect("first basic.consume");
                        let channel = channel.into_auto();
                        drop(consumer);

                        while conn.observe().read(|view| view.generation()).await == epoch {
                            timer
                                .sleep(Duration::from_millis(10))
                                .expect("generation wait")
                                .await;
                        }
                        timer
                            .sleep(Duration::from_millis(50))
                            .expect("topology replay wait")
                            .await;
                        let next = channel
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
                            .expect("recovered consumer capacity");
                        drop(next);
                        while !done.load(Ordering::Acquire) {
                            timer
                                .sleep(Duration::from_millis(10))
                                .expect("second cancel wait")
                                .await;
                        }
                    });
                    assert!(rt.block_on(run).is_ok(), "AMQP fiber must complete");
                },
                recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("recovery broker");
        },
    )
    .expect("recovery broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
