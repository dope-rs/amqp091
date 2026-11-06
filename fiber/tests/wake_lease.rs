use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
pub mod common;

use common::wake::probe::{Phase, Probe};
use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter;
use dope_test::harness::peer::TcpScript;

use crate::common::wake::{Ready, TUNE, with_client};

#[test]
fn keeps_linear() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            assert_eq!(stream.read(&mut [0]).expect("connection retirement"), 0);
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                1,
                |scope| {
                    let amqp = adapter::Adapter::new(scope.port());
                    let target = ((scope.block_on(Ready {
                        state: amqp.connection().observe(),
                        wait: fanout::Registration::new(),
                    }))
                    .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    let proof = (scope.block_on(Probe {
                        channels: amqp.channels(),
                        connection: amqp.connection(),
                        target: target.epoch(),
                        owner: fanout::Registration::new(),
                        contender: fanout::Registration::new(),
                        phase: Phase::Owner,
                    }))
                    .expect("AMQP fiber must complete");
                    assert!(proof.is_ok(), "wake ownership: {proof:?}");
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
    .expect("spawn lease broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
