use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
pub mod common;
use std::time::Duration;

use dope_amqp091::failure;
use dope_test::fibers::Yield;
use dope_test::harness::peer::AcceptScript;

use crate::common::recovery::TUNE;
use crate::common::recovery::client::with;

const SOURCES: [&str; 2] = [
    "https://www.rabbitmq.com/docs/reliability",
    "https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf",
];

#[test]
fn isolates_partial_frame() {
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
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("first Channel.OpenOk");
            Method::read(&mut first)
                .expect("request on opened channel")
                .check(1, 50, 30)
                .expect("Queue.Purge");
            first
                .write_all(&[1, 0, 1, 0])
                .expect("partial generation-1 frame");
            drop(first);

            let mut second = acceptor.accept().expect("recovery connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("recovery read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("recovery write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            Method::read(&mut second)
                .expect("Channel.Open")
                .check(1, 20, 10)
                .expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("replayed Channel.OpenOk");
            while second.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        move |broker| {
            with(
                broker.addr(),
                move |rt| {
                    let conn = rt.connection();
                    assert!(
                rt.block_on(dope_fiber::fiber!('_ => async move {
                    let target = conn.observe().ready().await.expect("first ready");
                    let channel = conn
                        .channels()
                        .open_in(target, 1)
                        .await
                        .expect("first channel");
                    assert!(channel.queue().purge("q").await.is_err());

                    while conn
                        .observe()
                        .read(|view| view.ready_cycle())
                        .await
                        != Some(2)
                    {
                        Yield::new().await;
                    }
                    conn.observe().read(|view| {
                        assert!(view.is_ready(), "replayed topology must be ready");
                        let cause = (view.last_exit()).expect("generation-1 disconnect evidence");
                        assert_eq!(cause.generation(), 1);
                        assert_eq!(
                            cause.reason(),
                            failure::Reason::Remote,
                            "mid-frame I/O loss must remain recoverable per {SOURCES:?}"
                        );
                    }).await;
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
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
