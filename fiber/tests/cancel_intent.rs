use std::convert::Infallible;
use std::io::Write as _;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::platform::wake;
use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::Table;
use dope_amqp091_fiber::channel::basic;
use dope_fiber::task::time::sleep::TimerExt as _;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once, wait};
pub mod common;

use dope_test::harness::peer::AcceptScript;

use crate::common::cancel::*;
use crate::common::client::check_no_frame;

#[test]
fn drops_uncommitted() {
    let (observed, notify) = wake::Ends::blocking().expect("broker observations").split();
    let (dropped, release) = wake::Ends::blocking().expect("drop barrier").split();
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
                .check(1, 60, 10)
                .expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 60, 11, &[]).expect("AMQP frame fixture"))
                .expect("first basic.qos-ok");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 60, 20)
                .expect("AMQP method route");
            notify.notify().expect("consume request");
            dropped
                .wait_timeout(Duration::from_secs(10))
                .expect("future drop");
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
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 60, 10)
                .expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 60, 11, &[]).expect("AMQP frame fixture"))
                .expect("recovered basic.qos-ok");
            check_no_frame(&second, "uncommitted consumer replay").expect("AMQP fixture");
            notify.notify().expect("replay observation");

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
            notify.notify().expect("replacement cancel-ok written");
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
                        let target = conn
                            .observe().ready()
                            .await
                            .expect("first ready");
                        let epoch = conn.observe().read(|view| view.generation()).await;
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
                        wait(&observed).await.expect("request wait");
                        drop(op);
                        release.notify().expect("future dropped");

                        while conn.observe().read(|view| view.generation()).await == epoch {
                            timer
                                .sleep(Duration::from_millis(10))
                                .expect("generation wait")
                                .await;
                        }
                        wait(&observed).await.expect("no replay wait");
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
                        wait(&observed)
                            .await
                            .expect("replacement cancel-ok written");
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
            broker.finish().expect("transport intent broker");
        },
    )
    .expect("transport intent broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
