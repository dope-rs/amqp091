use std::thread;

use crate::common::amqp::{Greeting, Peer, app::Endpoint};
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;
use std::time::Duration;

use common::recovery::Profile;
use dope_amqp091::config;
use dope_fiber::task::time::clock;
use dope_test::harness::peer::AcceptScript;
use o3::base::time::Positive;

use crate::common::recovery::TUNE;
use crate::common::recovery::client::POLICY;

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
            let preface = Peer::new(&mut first).preface().expect("AMQP preface");
            // The client publishes its lifetime before sending this preface.
            thread::sleep(DELAY);
            preface
                .start(Greeting::PLAIN)
                .expect("AMQP start")
                .recv()
                .expect("AMQP start ok")
                .tune(TUNE)
                .expect("AMQP tune")
                .recv()
                .expect("AMQP tune ok")
                .recv()
                .expect("AMQP open")
                .send()
                .expect("AMQP open ok");
            while first.read(&mut [0; 256]).expect("generation-1 retirement") != 0 {}
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
            while second.read(&mut [0; 256]).expect("endpoint teardown") != 0 {}
        },
        |broker| {
            Endpoint::new(
                broker.addr(),
                config::Config::new("guest", "guest").expect("AMQP config"),
            )
            .try_check::<Profile, _>(
                POLICY.retire_after(AGE),
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
                complete,
            );
            broker.finish().expect("scripted broker thread");
        },
    )
    .expect("scripted broker");
}
