use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
pub mod common;

use dope_test::harness::peer;
use std::time::Duration;

use dope::core::platform::wake;
use dope_amqp091::{config, failure};
use dope_test::harness::peer::AcceptScript;

use crate::common::recovery::TUNE;
use crate::common::recovery::client::with_config;

#[test]
fn rejects_reconnect() {
    let (wait, ready) = wake::Ends::blocking().expect("initial epoch gate").split();
    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(5)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(5)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            wait.wait_timeout(Duration::from_secs(10))
                .expect("initial AMQP epoch readiness");
            drop(first);

            let rejected = broker.accept().expect("rejected reconnect");
            rejected
                .read_wait(Some(Duration::from_secs(5)))
                .expect("rejected read timeout");
            let mut byte = [0_u8; 1];
            match rejected.read(&mut byte) {
                Ok(read) => assert_eq!(read, 0, "rejected transport emitted a protocol byte"),
                Err(error) => assert!(matches!(error, peer::Error::Syscall(error) if error.is_reset() || error.is_aborted())),
            }
        },
        |broker| {
            let config = config::Config::new("guest", "guest")
                .expect("config")
                .capacity(config::Capacity::default().causes(1));
            with_config(
                broker.addr(),
                config,
                |rt| {
                    let conn = rt.connection();
                    let target = ((rt.block_on(conn.observe().epoch()))
                        .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    ready
                        .notify()
                        .expect("publish initial AMQP epoch readiness");
                    assert!(
                        matches!(rt.block_on(conn.observe().retired(target)), Ok(Ok(_))),
                        "AMQP operation must succeed"
                    );
                    let error = ((rt.block_on(conn.observe().epoch()))
                        .expect("retained cause must exhaust a one-cell cause pool"))
                    .expect_err("retained cause must exhaust a one-cell cause pool");
                    std::assert_matches!(
                        error,
                        failure::Runtime::Local(failure::Error::Capacity(
                            failure::wire::Capacity::TerminalCause
                        ))
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
