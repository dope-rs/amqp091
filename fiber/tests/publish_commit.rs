use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_test::alloc;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Frame, Method};
pub mod common;

use common::publish::commit::Commit;
use dope_amqp091::adapter;
use dope_amqp091::message::property::Properties;
use dope_test::alloc::Tracker;
use dope_test::harness::peer::TcpScript;

use crate::common::publish::{TUNE, with_client};

#[test]
fn commits_without_alloc() {
    TcpScript::with(
        |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 20, 10)
                .expect("AMQP method route");
            stream
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");
            Method::read(stream)
                .expect("AMQP method")
                .check(1, 85, 10)
                .expect("AMQP method route");
            stream
                .write_all(&Method::try_encode(1, 85, 11, &[]).expect("AMQP frame fixture"))
                .expect("confirm.select-ok");

            let sent = Method::read(stream).expect("AMQP method");
            sent.check(1, 60, 40).expect("AMQP method route");
            let Frame(kind, ch, header) = Frame::read(&mut *stream).expect("content header");
            assert_eq!((kind, ch), (2, 1));
            assert_eq!(
                u64::from_be_bytes(
                    header[4..12]
                        .try_into()
                        .expect("content header body size is eight bytes"),
                ),
                1
            );
            let Frame(kind, ch, body) = Frame::read(&mut *stream).expect("content body");
            assert_eq!((kind, ch, body.as_slice()), (3, 1, b"x".as_slice()));

            let mut ack = 1u64.to_be_bytes().to_vec();
            ack.push(0);
            stream
                .write_all(&Method::try_encode(1, 60, 80, &ack).expect("AMQP frame fixture"))
                .expect("basic.ack tag one");
        },
        |broker| {
            let addr = broker.addr();

            with_client(
                addr,
                |app| {
                    let props = Properties::default();
                    let amqp = adapter::Adapter::new(app.port());
                    let client = app.connection();
                    let target = ((app.block_on(client.observe().ready()))
                        .expect("AMQP operation must succeed"))
                    .expect("AMQP operation must succeed");
                    assert!(
                        matches!(app.block_on(Commit::new(amqp, target, &props)), Ok(Ok(_))),
                        "AMQP operation must succeed"
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
            broker.finish().expect("broker completion");
        },
    )
    .expect("broker");
}

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
