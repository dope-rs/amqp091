use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::app::{Endpoint, Policy};
use crate::common::amqp::{Peer, Tune};
use crate::common::client::close_reason;
pub mod common;

use std::sync::mpsc;
use std::time::Duration;

use dope::core::io::Reclaimed;
use dope::manifold::timing;
use dope_amqp091::failure::wire;
use dope_amqp091::protocol::wire::reader;
use dope_amqp091::protocol::wire::text::resident;
use dope_amqp091::{config, failure};
use dope_test::alloc::Tracker;
use dope_test::harness::peer::TcpScript;
use wire::field;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

fn close(
    args: &[u8],
    verify: impl for<'b, 'd> FnOnce(&failure::Error<resident::Short<'b, 'd>>),
) -> usize {
    use crate::common::amqp::wire::Method;

    let (release, begin) = mpsc::channel();
    let (finish, wait) = mpsc::channel();
    TcpScript::with(
        move |stream| {
            assert!(
                Peer::new(&mut *stream)
                    .open(Tune::new(16, 131_072, 0))
                    .is_ok()
            );
            assert!(begin.recv_timeout(Duration::from_secs(5)).is_ok());
            assert!(Method::write(stream, 0, 10, 50, args).is_ok());
            assert!(wait.recv_timeout(Duration::from_secs(5)).is_ok());
        },
        |broker| {
            let config = (config::Config::new("guest", "guest"))
                .expect("the fixture credentials must fit the wire limits");
            let policy = Policy::new(1, 8, Duration::from_secs(5));
            let allocs = {
                let mut reports = Vec::new();
                let allocs = Endpoint::new(broker.addr(), config)
                    .enter::<timing::Throughput, _, _>(
                        policy,
                        |scope| {
                            let conn = scope.connection();
                            let ready = ((scope.block_on(conn.observe().ready()))
                                .expect("the server must complete its handshake before closing"))
                            .expect("the server must complete its handshake before closing");
                            assert!(release.send(()).is_ok());
                            let ((), allocs) = Tracker::measure(|| {
                                let observed =
                                    scope.block_on(dope_fiber::fiber!('_ => async move {
                                        let error = loop {
                                            match conn.observe().lease(ready).await {
                                                Ok(_) => dope_test::fibers::Yield::new().await,
                                                Err(error) => break error,
                                            }
                                        };
                                        verify(error.error());
                                    }));
                                assert!(observed.is_ok(), "close observation failed: {observed:?}");
                            });
                            assert!(finish.send(()).is_ok());
                            allocs[0]
                        },
                        |event| reports.push(event),
                        recover,
                        |result| {
                            *result
                                .as_ref()
                                .map_err(|error| error.kind())
                                .expect("endpoint completion")
                        },
                    );
                assert!(
                    matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
                    "unexpected reclaimed AMQP completion: {reports:?}"
                );
                allocs
            };
            assert!(broker.finish().is_ok());
            allocs
        },
    )
    .expect("the isolated broker must bind an ephemeral port")
}

fn retained(error: &failure::Error<resident::Short<'_, '_>>, expected: &str) {
    let (code, text, class, method) = match error {
        failure::Error::Server {
            reply_code: code,
            reply_text: text,
            class_id: class,
            method_id: method,
        } => Some((code, text, class, method)),
        _ => None,
    }
    .expect("the server failure must retain its exact reply fields");
    assert_eq!((*code, *class, *method), (320, 0, 0));
    assert_eq!(text.as_ref(), expected);
    assert_eq!(size_of_val(text), 3 * size_of::<usize>());
}

#[test]
fn copies_once() {
    let empty = close(&close_reason("").expect("close reason"), |error| {
        retained(error, "")
    });
    let utf8 = close(&close_reason("αβ").expect("close reason"), |error| {
        retained(error, "αβ")
    });
    assert_eq!(
        utf8,
        empty + 1,
        "nonempty text must add exactly one allocation"
    );
}

#[test]
fn retains_maximum() {
    let text = "x".repeat(255);
    close(&close_reason(&text).expect("close reason"), |error| {
        retained(error, &text)
    });
}

#[test]
fn rejects_utf8() {
    close(&[1, 64, 2, b'a', 0xff, 0, 0, 0, 0], |failure| {
        let (offset, relative, error) = match failure {
            failure::Error::Decode(reader::Fault {
                offset,
                error:
                    reader::Error::Utf8 {
                        offset: relative,
                        error,
                    },
            }) => Some((offset, relative, error)),
            _ => None,
        }
        .expect("invalid UTF-8 must preserve the field and decoder offsets");
        assert_eq!((*offset, *relative), (3, 0));
        assert_eq!(error.valid_up_to(), 1);
        assert_eq!(error.error_len(), Some(1));
    });
}

#[test]
fn rejects_nul() {
    close(&[1, 64, 3, b'a', 0, b'b', 0, 0, 0, 0], |error| {
        assert!(matches!(
            error,
            failure::Error::Protocol(wire::Fault::TextNul {
                kind: wire::Encoding::Short,
                offset: 4,
            })
        ));
    });
}

#[test]
fn rejects_truncation() {
    close(&[1, 64, 3, b'a'], |error| {
        assert!(matches!(
            error,
            failure::Error::Decode(reader::Fault {
                offset: 3,
                error: reader::Error::Read {
                    offset: 0,
                    requested: 3,
                    remaining: 1
                },
            })
        ));
    });
}

#[test]
fn rejects_trailing() {
    let mut args = close_reason("a").expect("close reason");
    args.push(0);
    close(&args, |error| {
        assert!(matches!(
            error,
            failure::Error::Field(field::Fault::Trailing {
                offset: 8,
                remaining: 1,
            })
        ));
    });
}

#[test]
fn rejects_class() {
    close(&[1, 64, 1, b'a', 0], |error| {
        assert!(matches!(
            error,
            failure::Error::Decode(reader::Fault {
                offset: 4,
                error: reader::Error::Read {
                    offset: 0,
                    requested: 2,
                    remaining: 1
                },
            })
        ));
    });
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
