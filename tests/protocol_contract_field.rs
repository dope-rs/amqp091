pub mod common;
use common::{canonical_table, header};
use dope_amqp091::failure::{Error, wire};
use dope_amqp091::message::field::{Value, view};
use dope_amqp091::protocol::wire::content::Header;
use dope_amqp091::protocol::wire::reader;

#[test]
fn accepts_field_tags() {
    for tag in u8::MIN..=u8::MAX {
        let (bytes, valid) = common::tagged_header(tag).expect("field tag fixture");
        let result = Header::decode(bytes);
        if !valid {
            assert!(matches!(
                result,
                Err(Error::Field(wire::field::Fault::Tag { offset: 6, actual }))
                    if actual == tag
            ));
            continue;
        }
        let (_, props) = result.expect("admitted field tag").into_parts();
        let mut fields = props.headers().expect("headers flag").iter();
        let (key, value) = fields.next().expect("tagged field");
        assert_eq!(key, "k");
        let matched = match (tag, value) {
            (b't', view::Value::Bool(true))
            | (b'b', view::Value::I8(i8::MIN))
            | (b'B', view::Value::U8(u8::MAX))
            | (b'U' | b's', view::Value::I16(-2))
            | (b'u', view::Value::U16(65_534))
            | (b'I', view::Value::I32(-2))
            | (b'i', view::Value::U32(4_294_967_294))
            | (b'l', view::Value::I64(-2))
            | (b'L', view::Value::U64(18_446_744_073_709_551_614))
            | (b'T', view::Value::Timestamp(18_446_744_073_709_551_614))
            | (b'f', view::Value::F32(1.5))
            | (b'd', view::Value::F64(1.5))
            | (
                b'D',
                view::Value::Decimal {
                    scale: 2,
                    value: -2,
                },
            )
            | (b'S', view::Value::String(&[0xff, 0]))
            | (b'x', view::Value::Bytes(&[0xff, 0]))
            | (b'V', view::Value::Void) => true,
            (b'A', view::Value::Array(array)) => {
                let mut values = array.iter();
                matches!(values.next(), Some(view::Value::Bool(true)))
                    && matches!(values.next(), Some(view::Value::Void))
                    && values.next().is_none()
            }
            (b'F', view::Value::Table(table)) => {
                let mut values = table.iter();
                matches!(values.next(), Some(("n", view::Value::Bool(true))))
                    && values.next().is_none()
            }
            _ => false,
        };
        assert!(matched, "field tag {tag} decoded as another value");
        assert!(matches!(fields.next(), Some(("z", view::Value::Void))));
        assert!(fields.next().is_none());
    }
}

#[test]
fn keeps_nested_tag_offset() {
    let bytes = header(1 << 13, common::NESTED_INVALID_TAG).expect("nested tag fixture");
    assert!(matches!(
        Header::decode(bytes),
        Err(Error::Field(wire::field::Fault::Tag {
            offset: 18,
            actual: b'?'
        }))
    ));
}

#[test]
fn validates_wire_keys() {
    let bytes =
        header(1 << 13, &[0, 0, 0, 4, 2, 0xce, 0xb1, b'V']).expect("Unicode field key fixture");
    let (_, props) = Header::decode(bytes)
        .expect("valid Unicode field key")
        .into_parts();
    assert!(matches!(
        props.headers().expect("headers flag").iter().next(),
        Some(("α", view::Value::Void))
    ));

    let bytes =
        header(1 << 13, &[0, 0, 0, 4, 2, 0, 0xff, b'V']).expect("malformed field key fixture");
    assert!(matches!(
        Header::decode(bytes).err().expect("UTF-8 validation precedes NUL"),
        Error::Field(wire::field::Fault::Read(reader::Fault {
            offset: 4,
            error: reader::Error::Utf8 { offset: 1, error },
        })) if error.valid_up_to() == 1 && error.error_len() == Some(1)
    ));

    for (data, offset) in [
        ([0, 0, 0, 4, 2, b'x', 0, b'V'].as_slice(), 6),
        (
            [0, 0, 0, 10, 1, b'n', b'F', 0, 0, 0, 3, 1, 0, b'V'].as_slice(),
            12,
        ),
    ] {
        let bytes = header(1 << 13, data).expect("NUL field key fixture");
        assert!(matches!(
            Header::decode(bytes).err().expect("reject NUL field key"),
            Error::Protocol(wire::Fault::TextNul {
                kind: wire::Encoding::Short,
                offset: actual,
            }) if actual == offset
        ));
    }
}

#[test]
fn preserves_decimal_sign() {
    let value = Value::decimal(2, -12_345);
    assert!(matches!(
        value.view(),
        view::Value::Decimal {
            scale: 2,
            value: -12_345
        }
    ));
}

#[test]
fn orders_and_replaces() {
    for reverse in [false, true] {
        let value = (canonical_table(reverse)).expect("valid canonical table, reverse={reverse}");
        let (table,) = match value.view() {
            view::Value::Table(table) => Some((table,)),
            _ => None,
        }
        .expect("expected table");
        let entries = table.iter().collect::<Vec<_>>();
        assert_eq!(
            entries.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            ["a-nested", "m-replaced", "z-id"]
        );
        assert!(matches!(entries[1].1, view::Value::Bool(true)));
        assert!(matches!(entries[2].1, view::Value::I32(7)));

        let (nested,) = match entries[0].1 {
            view::Value::Table(nested) => Some((nested,)),
            _ => None,
        }
        .expect("expected nested table");
        let nested = nested.iter().collect::<Vec<_>>();
        assert!(matches!(
            nested.as_slice(),
            [("a", view::Value::I32(1)), ("z", view::Value::I32(2))]
        ));
    }
}
