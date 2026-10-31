//! Public-boundary checks derived directly from RabbitMQ's AMQP 0-9-1 reference.
//!
//! Source: <https://github.com/rabbitmq/amqp-0.9.1-spec/blob/main/docs/amqp-0-9-1-reference.md#basic>

use dope_amqp091::message::field::view;
use dope_amqp091::protocol::wire::content::Header;
use o3::buffer::storage;

#[test]
fn retains_properties() {
    let table = [1, b'x', b'S', 0, 0, 0, 3, b'a', b'b', b'c'];
    let mut bytes = Vec::with_capacity(42);
    bytes.extend_from_slice(&60_u16.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&7_u64.to_be_bytes());
    bytes.extend_from_slice(
        &((1_u16 << 15) | (1_u16 << 13) | (1_u16 << 12) | (1_u16 << 11)).to_be_bytes(),
    );
    bytes.extend_from_slice(&[
        10, b't', b'e', b'x', b't', b'/', b'p', b'l', b'a', b'i', b'n',
    ]);
    bytes.extend_from_slice(&(table.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&table);
    bytes.extend_from_slice(&[2, 9]);

    let (size, retained) = {
        let source = (storage::Shared::try_copy(&bytes)).expect("header fixture storage");
        let (size, props) = (Header::decode(source).map(Header::into_parts))
            .expect("official basic content header shape");
        (size, props)
    };

    assert_eq!(size, 7);
    assert_eq!(retained.content_type(), Some("text/plain"));
    assert_eq!(retained.delivery(), Some(2));
    assert_eq!(retained.priority(), Some(9));
    let headers = (retained.headers()).expect("headers flag");
    let mut fields = headers.iter();
    let (name, value) = (fields.next()).expect("one field");
    assert_eq!(name, "x");
    assert!(matches!(value, view::Value::String(b"abc")));
    assert!(fields.next().is_none());
}
