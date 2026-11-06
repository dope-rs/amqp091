use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope::manifold::timing;

use crate::common::amqp::Peer;
use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::Method;
pub mod common;

use dope_test::harness::peer;
use std::time::Duration;

use dope_amqp091::{config, failure};
use dope_test::harness::peer::AcceptScript;

use crate::common::recovery::TUNE;
use crate::common::recovery::client::POLICY;

#[test]
fn rejects_reconnect() {
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
            Method::read(&mut first)
                .expect("initial ready channel request")
                .check(1, 20, 10)
                .expect("Channel.Open");
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
            Endpoint::new(broker.addr(), config)
                .shutdown(broker.completion())
                .try_check::<timing::Throughput, _>(
                POLICY,
                |rt| {
                    let conn = rt.connection();
                    let error = rt.until_shutdown(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("initial epoch");
                        assert!(conn.channels().open_in(target, 1).await.is_err());
                        conn.observe().retired(target.epoch()).await.expect("retired epoch");
                        conn.observe().epoch().await
                    }))
                    .expect("peer shutdown")
                    .expect("reconnect drive")
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
