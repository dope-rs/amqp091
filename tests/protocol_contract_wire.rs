use dope_amqp091::protocol::wire::{Write as _, Writer};
pub mod common;
use common::Sink;
use dope::manifold::client::session::codec::{
    self, Codec as _, Input, Parse, Stateless as _, staging::Policy as _,
};
use dope_amqp091::message::field;
use dope_amqp091::protocol::method::{Encode as _, basic, conn};
use dope_amqp091::protocol::wire::frame::{Codec, Fault, Inbound, Violation};
use dope_amqp091::protocol::wire::text::{long, short};
use o3::base::cell::region;
use o3::buffer::{pool, resident::Domain, storage::Shared};
use std::pin::Pin;

#[test]
fn owns_borrowed_frames() {
    let mut publish = Sink::default();
    {
        let exchange = String::from("ex");
        let key = String::from("rk");
        let (exchange, key) = match (short::Short::new(&exchange), short::Short::new(&key)) {
            (Ok(exchange), Ok(key)) => Some((exchange, key)),
            _ => None,
        }
        .expect("fixture names must be valid short strings");
        basic::Publish {
            exchange,
            routing_key: key,
            mandatory: true,
            immediate: false,
        }
        .write(&mut publish, 7);
    }
    assert_eq!(
        publish.0,
        [
            1, 0, 7, 0, 0, 0, 13, 0, 60, 0, 40, 0, 0, 2, b'e', b'x', 2, b'r', b'k', 1, 0xCE,
        ]
    );

    let mut start_ok = Sink::default();
    {
        let sasl = String::from("PLAIN");
        let response = vec![0, b'u', 0, b'p'];
        let locale = String::from("en_US");
        let props = field::Table::new();
        let (sasl, response, locale) = match (
            short::Short::new(&sasl),
            long::Long::new(&response),
            short::Short::new(&locale),
        ) {
            (Ok(sasl), Ok(response), Ok(locale)) => Some((sasl, response, locale)),
            _ => None,
        }
        .expect("fixture handshake fields must be encodable");
        conn::StartOk {
            client_properties: &props,
            mechanism: sasl,
            response,
            locale,
        }
        .write(&mut start_ok, 0);
    }
    assert_eq!(
        start_ok.0,
        [
            1, 0, 0, 0, 0, 0, 28, 0, 10, 0, 11, 0, 0, 0, 0, 5, b'P', b'L', b'A', b'I', b'N', 0, 0,
            0, 4, 0, b'u', 0, b'p', 5, b'e', b'n', b'_', b'U', b'S', 0xCE,
        ]
    );
}

#[test]
fn retains_first_failure() {
    let pool = (pool::Pool::try_new(1, 4)).expect("the transaction fixture must allocate");
    let cursor = (pool.cursor()).expect("the transaction fixture owns a free slot");
    let mut out = Writer::new(cursor);
    out.extend(b"ab");
    out.extend(b"cde");
    out.push(b'f');
    let error = (out.status()).expect_err("the first rejected slice must remain observable");
    let (required, room) = error.into_parts();
    assert_eq!((required.get(), room), (3, 2));
    assert_eq!(out.as_mut(), b"ab");
    let failed = (out.try_finish())
        .err()
        .expect("a rejected writer must never become a payload");
    assert_eq!(failed, error);
}

#[test]
fn rejects_bad_envelopes() {
    let mut domain = Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let domain = domain.get_mut();
        let budget = domain
            .try_budget(0, token.proof())
            .expect("empty resident domain");
        let codec = Codec::new(4096);
        let plan = <Codec as codec::Codec>::Staging::plan(codec.staging()).expect("codec staging");
        let limit = <Codec as codec::Codec>::Staging::limit(plan);
        for (bytes, expected, count) in [
            (
                b"\x08\0\x01\0\0\0\0\xce".as_slice(),
                Fault::HeartbeatChannel { channel: 1 },
                7,
            ),
            (b"\x08\0\0\0\0\0\x01", Fault::HeartbeatPayload { len: 1 }, 7),
            (
                b"\x02\0\0\0\0\0\0\xce",
                Fault::ContentChannel {
                    frame_type: 2,
                    channel: 0,
                },
                7,
            ),
            (
                b"\x03\0\0\0\0\0\0\xce",
                Fault::ContentChannel {
                    frame_type: 3,
                    channel: 0,
                },
                7,
            ),
            (
                b"\x63\0\0\0\0\0\0",
                Fault::Violation(Violation::UndefinedType(99)),
                7,
            ),
            (
                b"\x01\0\0\0\0\0\0\0",
                Fault::Violation(Violation::InvalidEnd(0)),
                8,
            ),
        ] {
            let source = Shared::try_copy(bytes).expect("envelope fixture storage");
            Input::with(&source, &budget, limit, |input| {
                assert!(matches!(
                    codec.parse(input),
                    Ok(Parse::Item { head: Inbound::Fault(actual), consumed })
                        if actual == expected && consumed.count().get() as usize == count
                ));
            });
        }
        let heartbeat =
            Shared::try_copy(b"\x08\0\0\0\0\0\0\xce").expect("envelope fixture storage");
        Input::with(&heartbeat, &budget, limit, |input| {
            assert!(matches!(
                codec.parse(input),
                Ok(Parse::Item { head: Inbound::Heartbeat, consumed })
                    if consumed.count().get() == 8
            ));
        });
        assert_eq!(budget.resident(), 0);
    });
}
