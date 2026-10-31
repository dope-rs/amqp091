use std::convert::Infallible;
use std::thread;

use dope::core::driver::life::recovery;

use crate::common::amqp::Peer;
pub mod common;
use std::io;
use std::time::Duration;

use common::recovery::Profile;
use dope_amqp091::config;
use dope_fiber::task::time::clock;
use dope_test::harness::peer::AcceptScript;
use o3::base::time::Positive;

use crate::common::recovery::TUNE;
use crate::common::recovery::client::with_policy;

#[test]
fn owns_deadline() {
    const AGE: Positive = Positive::from_secs::<2>();
    const DELAY: Duration = Duration::from_millis(500);

    AcceptScript::with(
        move |broker| {
            let mut first = broker.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            thread::sleep(DELAY);
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");
            io::copy(&mut first, &mut io::sink()).expect("generation-1 retirement");
            drop(first);

            let mut second = broker.accept().expect("second connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            io::copy(&mut second, &mut io::sink()).expect("endpoint teardown");
        },
        |broker| {
            with_policy::<Profile>(
                broker.addr(),
                config::Config::new("guest", "guest").expect("AMQP config"),
                AGE,
                |rt| {
                    let conn = rt.connection();
                    assert!(
                        rt.block_on(dope_fiber::fiber!('_ => async move {
                            let ready = conn.observe().ready().await.expect("first ready");

                            let first = conn
                                .observe()
                                .lease(ready)
                                .await
                                .expect("generation-1 ready")
                                .expect("generation-1 lease");
                            let remaining = first
                                .expires_at()
                                .try_left(clock::Clock::now().await)
                                .unwrap_or(Duration::ZERO);
                            assert!(first.ready() == ready);
                            assert!(first.epoch() == ready.epoch());
                            assert_eq!(first.generation(), 1);
                            assert!(
                                remaining <= AGE.get().saturating_sub(DELAY),
                                "handshake time was incorrectly added back: {:?}",
                                remaining
                            );

                            conn
                                .observe()
                                .retired(first.epoch())
                                .await
                                .expect("generation-1 retirement");
                            let ready = conn.observe().ready().await.expect("generation-2 ready");
                            let second = conn
                                .observe()
                                .lease(ready)
                                .await
                                .expect("generation-2 ready")
                                .expect("generation-2 lease");
                            assert!(second.ready() == ready);
                            assert!(second.epoch() != first.epoch());
                            assert_eq!(second.generation(), 2);
                            assert!(
                                second.expires_at() > first.expires_at(),
                                "a new physical generation must own a fresh absolute deadline"
                            );
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
