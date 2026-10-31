pub mod common;
use common::header;
use dope_amqp091::failure::wire::property;
use dope_amqp091::failure::{Error, wire};
use dope_amqp091::message::Builder;
use dope_amqp091::protocol::wire::content::Header;
use dope_amqp091::protocol::wire::reader;
use o3::buffer::storage::Shared;

const CONTENT_TYPE_FLAG: u16 = 1 << 15;
const DELIVERY_MODE_FLAG: u16 = 1 << 12;
const PRIORITY_FLAG: u16 = 1 << 11;
const RESERVED_FLAG: u16 = 1 << 2;

#[test]
fn admits_complete_text() {
    let bytes = header(CONTENT_TYPE_FLAG, &[2, b'a']).expect("truncated property fixture");
    std::assert_matches!(
        Header::decode(bytes)
            .err()
            .expect("reject truncated property"),
        Error::Field(wire::field::Fault::Read(reader::Fault {
            offset: 0,
            error: reader::Error::Read {
                offset: 1,
                requested: 2,
                remaining: 1
            },
        }))
    );
    let bytes = header(CONTENT_TYPE_FLAG, &[2, 0, 0xff]).expect("invalid UTF-8 fixture");
    std::assert_matches!(
        Header::decode(bytes).err().expect("reject invalid property UTF-8"),
        Error::Field(wire::field::Fault::Read(reader::Fault {
            offset: 0,
            error: reader::Error::Utf8 { offset: 1, error },
        })) if error.valid_up_to() == 1 && error.error_len() == Some(1)
    );
    let bytes = header(CONTENT_TYPE_FLAG, &[1, 0]).expect("NUL property fixture");
    std::assert_matches!(
        Header::decode(bytes).err().expect("reject property NUL"),
        Error::Protocol(wire::Fault::TextNul {
            kind: wire::Encoding::Short,
            offset: 1
        })
    );
}

#[test]
fn preserves_field_offsets() {
    let flags = CONTENT_TYPE_FLAG | 1 << 6;
    let data = [1, b'a', 0, 0, 0, 0, 0, 0, 0, 42];
    let bytes = header(flags, &data).expect("complete property fixture");
    let (_, props) = Header::decode(bytes)
        .expect("complete properties")
        .into_parts();
    assert_eq!(props.content_type(), Some("a"));
    assert_eq!(props.timestamp, Some(42));

    let bytes = header(flags, &data[..data.len() - 1]).expect("truncated timestamp fixture");
    std::assert_matches!(
        Header::decode(bytes)
            .err()
            .expect("reject truncated timestamp"),
        Error::Field(wire::field::Fault::Read(reader::Fault {
            offset: 2,
            error: reader::Error::Read {
                offset: 0,
                requested: 8,
                remaining: 7
            },
        }))
    );
}

#[test]
fn preserves_nested_offset() {
    let data = [0, 0, 0, 12, 1, b'a', b'A', 0, 0, 0, 5, b'D', 2, 0, 0, 0];
    let bytes = header(1 << 13, &data).expect("nested decimal fixture");
    std::assert_matches!(
        Header::decode(bytes)
            .err()
            .expect("reject truncated decimal"),
        Error::Field(wire::field::Fault::Read(reader::Fault {
            offset: 13,
            error: reader::Error::Read {
                offset: 0,
                requested: 4,
                remaining: 3
            },
        }))
    );
}

#[test]
fn rejects_delivery_mode() {
    assert!(matches!(
        Builder::default().delivery(0),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::DeliveryMode { actual: 0 }
        )))
    ));
    assert!(matches!(
        Builder::default().delivery(3),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::DeliveryMode { actual: 3 }
        )))
    ));
    assert!(Builder::default().delivery(1).is_ok());
    assert!(Builder::default().delivery(2).is_ok());

    let bytes = (header(DELIVERY_MODE_FLAG, &[3])).expect("property fixture allocation");
    assert!(matches!(
        Header::decode(bytes),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::DeliveryMode { actual: 3 }
        )))
    ));
}

#[test]
fn checks_priority() {
    assert!(Builder::default().priority(0).is_ok());
    assert!(Builder::default().priority(9).is_ok());
    assert!(matches!(
        Builder::default().priority(10),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::Priority { actual: 10, max: 9 }
        )))
    ));

    let bytes = (header(PRIORITY_FLAG, &[10])).expect("property fixture allocation");
    assert!(matches!(
        Header::decode(bytes),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::Priority { actual: 10, max: 9 }
        )))
    ));
}

#[test]
fn requires_empty_reserved() {
    assert!(Builder::default().reserved("").is_ok());
    assert!(matches!(
        Builder::default().reserved("reserved"),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::Reserved { actual: 8 }
        )))
    ));

    let bytes = (header(RESERVED_FLAG, &[1, b'x'])).expect("property fixture allocation");
    assert!(matches!(
        Header::decode(bytes),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::Reserved { actual: 1 }
        )))
    ));
}

#[test]
fn rejects_trailing_header() {
    let payload = header(0, &[0xff]).expect("malformed content header fixture allocation");
    assert!(matches!(
        Header::decode(payload),
        Err(Error::Protocol(wire::Fault::Property(
            property::Fault::Trailing {
                offset: 0,
                remaining: 1,
            }
        )))
    ));
}

#[test]
fn rejects_unknown_flags() {
    for flags in [1, 2] {
        let payload = header(flags, &[]).expect("property flags fixture");
        assert!(matches!(
            Header::decode(payload),
            Err(Error::Protocol(wire::Fault::Property(property::Fault::Flags {
                actual,
                allowed: 0xfffc,
            }))) if actual == flags
        ));
    }
}

#[test]
fn rejects_header_prefix() {
    let short = Shared::try_copy(&[0; 13]).expect("truncated header fixture");
    let Err(Error::Protocol(wire::Fault::ContentSize(error))) = Header::decode(short) else {
        panic!("expected truncated content header");
    };
    let (required, available) = error.into_parts();
    assert_eq!((required.get(), available), (14, 13));

    for (class, weight, expected) in [
        (0u16, 0u16, wire::Fault::ContentClass { actual: 0 }),
        (60, 1, wire::Fault::ContentWeight { actual: 1 }),
    ] {
        let mut bytes = [0; 14];
        bytes[..2].copy_from_slice(&class.to_be_bytes());
        bytes[2..4].copy_from_slice(&weight.to_be_bytes());
        let payload = Shared::try_copy(&bytes).expect("content prefix fixture");
        assert!(matches!(
            Header::decode(payload), Err(Error::Protocol(actual)) if actual == expected
        ));
    }
    assert!(Header::decode(header(0, &[]).expect("empty content header")).is_ok());
}
