use dope_amqp091::failure;
use dope_amqp091::failure::wire::field::Fault;
use dope_amqp091::message::field::{self, view};
use dope_amqp091::protocol::wire::content::Header;
use o3::buffer::storage;

#[test]
fn retains_backing() {
    let table = [
        1, b'a', b'A', 0, 0, 0, 3, b'V', b't', 1, 1, b's', b'S', 0, 0, 0, 3, b'a', b'b', b'c',
    ];
    let mut payload = Vec::new();
    payload.extend_from_slice(&60u16.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&7u64.to_be_bytes());
    payload.extend_from_slice(&(1u16 << 13).to_be_bytes());
    payload.extend_from_slice(&(table.len() as u32).to_be_bytes());
    payload.extend_from_slice(&table);

    let retained = {
        let data = (storage::Shared::try_copy(&payload)).expect("metadata fixture storage");
        let (size, props) =
            (Header::decode(data).map(Header::into_parts)).expect("valid content header");
        assert_eq!(size, 7);
        (props.try_clone()).expect("one retained metadata owner must fit")
    };

    let headers = (retained.headers()).expect("headers property");
    let mut entries = headers.iter();
    let (key, array) = match (entries.next()).expect("expected array header") {
        (key, view::Value::Array(array)) => Some((key, array)),
        _ => None,
    }
    .expect("expected array header");
    assert_eq!(key, "a");
    let values = array.iter().collect::<Vec<_>>();
    assert!(matches!(
        values.as_slice(),
        [view::Value::Void, view::Value::Bool(true)]
    ));
    let (key, value) = match (entries.next()).expect("expected string header") {
        (key, view::Value::String(value)) => Some((key, value)),
        _ => None,
    }
    .expect("expected string header");
    assert_eq!(key, "s");
    assert_eq!(value, b"abc");
    assert!(entries.next().is_none());
}

#[test]
fn rejects_depth() {
    let mut value = vec![b'V'];
    for _ in 0..64 {
        let mut nested = Vec::with_capacity(value.len() + 5);
        nested.push(b'A');
        nested.extend_from_slice(&(value.len() as u32).to_be_bytes());
        nested.extend_from_slice(&value);
        value = nested;
    }
    let mut table = Vec::with_capacity(value.len() + 2);
    table.extend_from_slice(&[1, b'k']);
    table.extend_from_slice(&value);
    let mut payload = Vec::new();
    payload.extend_from_slice(&60u16.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&0u64.to_be_bytes());
    payload.extend_from_slice(&(1u16 << 13).to_be_bytes());
    payload.extend_from_slice(&(table.len() as u32).to_be_bytes());
    payload.extend_from_slice(&table);

    let data = (storage::Shared::try_copy(&payload)).expect("nested fixture storage");
    assert!(matches!(
        Header::decode(data),
        Err(failure::Error::Field(Fault::Depth {
            actual: 65,
            max: 64
        }))
    ));
}

#[test]
fn rejects_nodes() {
    let values = vec![b'V'; 65_536];
    let mut table = Vec::with_capacity(values.len() + 7);
    table.extend_from_slice(&[1, b'k', b'A']);
    table.extend_from_slice(&(values.len() as u32).to_be_bytes());
    table.extend_from_slice(&values);
    let mut payload = Vec::new();
    payload.extend_from_slice(&60u16.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&0u64.to_be_bytes());
    payload.extend_from_slice(&(1u16 << 13).to_be_bytes());
    payload.extend_from_slice(&(table.len() as u32).to_be_bytes());
    payload.extend_from_slice(&table);

    let data = (storage::Shared::try_copy(&payload)).expect("node fixture storage");
    assert!(matches!(
        Header::decode(data),
        Err(failure::Error::Field(Fault::Nodes {
            actual: 65_537,
            max: 65_536
        }))
    ));
}

#[test]
fn bounds_owned_depth() {
    let mut value = field::Value::void();
    for _ in 0..62 {
        value = field::Value::array(vec![value]).expect("bounded array");
    }
    let mut table = field::Table::new();
    table.insert("deep", value).expect("nested entry");
    table
        .insert("shallow", field::Value::void())
        .expect("shallow entry");
    let value = field::Value::table(table).expect("maximum nested depth");
    assert!(matches!(
        field::Value::array(vec![value, field::Value::void()]),
        Err(failure::Error::Field(Fault::Depth {
            actual: 65,
            max: 64,
        }))
    ));
}
