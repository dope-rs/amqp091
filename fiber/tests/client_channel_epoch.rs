use std::convert::Infallible;
use std::io::Write as _;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::Method;
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use std::sync::mpsc;

use dope_test::fibers::Yield;
use dope_test::harness::peer::AcceptScript;

use crate::common::client::*;

#[test]
fn releases_detached_open() {
    let (req_tx, req_rx) = mpsc::channel();
    let (drop_tx, drop_rx) = mpsc::channel();
    let (clean_tx, clean_rx) = mpsc::channel();
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
                .check(1, 20, 10).expect("AMQP method route");
            req_tx.send(()).expect("publish detached open request");
            drop_rx.recv().expect("detached open drop");
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
            check_no_frame(&second, "detached open replay").expect("AMQP fixture");
            clean_tx.send(()).expect("publish clean replay observation");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("reused channel open-ok");
            Method::read(&mut second)
                .expect("connection close")
                .check(0, 10, 50).expect("AMQP method route");
            second
                .write_all(&Method::try_encode(0, 10, 51, &[]).expect("AMQP frame fixture"))
                .expect("connection close-ok");
        },
        |broker| {
            let chan = config::Channel::default()
                .methods(1)
                .confirms(1)
                .consumers(0)
                .deliveries(1)
                .returns(1);
            let cap = config::Capacity::default()
                .channels(1)
                .channel(chan)
                .egress(config::Egress::new(2, 4096));
            let cfg = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(cap);

            with(
                broker.addr(),
                cfg,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                let target = conn.observe().ready().await.expect("first ready");
                let epoch = conn.observe().read(|view| view.generation()).await;
                let mut opening = Pinned::new(conn.channels().open_in(target, 1));
                assert!(poll_once(opening.as_pin()).await.is_pending());
                loop {
                    match req_rx.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before observing detached open"), }
                }
                drop(opening);
                drop_tx.send(()).expect("publish detached open drop");

                while conn.observe().read(|view| view.generation()).await == epoch {
                    Yield::new().await;
                }
                loop {
                    match clean_rx.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before clean replay observation"), }
                }
                conn.channels().open(1).await.expect("generation reuse");
                let ready = conn.observe().ready().await.expect("recovered ready");
                conn.control()
                    .close(ready, 200, "done")
                    .await
                    .expect("connection close");
                })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("open generation broker");
        },
    )
    .expect("open generation broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
