use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_amqp091::message::field::{Table, Value};
use dope_amqp091::service::Spec;
use dope_amqp091::{config, failure};
use o3::buffer::storage;

pub mod check;
pub mod settings;
mod sink;

pub use sink::Sink;

pub fn plan(capacity: config::Capacity) -> Result<Spec, failure::Error> {
    let config = config::Config::new("guest", "guest")?.capacity(capacity);
    Spec::try_new(config, 1, 1)
}

pub fn canonical_table(reverse: bool) -> Result<Value, failure::Error> {
    let mut nested = Table::new();
    let mut entries = [("a", Value::from(1i32)), ("z", Value::from(2i32))];
    if reverse {
        entries.reverse();
    }
    for (key, value) in entries {
        nested.insert(key, value)?;
    }

    let mut table = Table::new();
    let mut entries = [
        ("a-nested", Value::table(nested)?),
        ("m-replaced", Value::string(b"obsolete".to_vec())?),
        ("z-id", Value::from(7i32)),
    ];
    if reverse {
        entries.reverse();
    }
    for (key, value) in entries {
        table.insert(key, value)?;
    }
    table.insert("m-replaced", Value::from(true))?;
    Value::table(table)
}

pub fn header(flags: u16, bytes: &[u8]) -> Result<storage::Shared, storage::AllocError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&60_u16.to_be_bytes());
    payload.extend_from_slice(&0_u16.to_be_bytes());
    payload.extend_from_slice(&0_u64.to_be_bytes());
    payload.extend_from_slice(&flags.to_be_bytes());
    payload.extend_from_slice(bytes);
    storage::Shared::try_copy(&payload)
}

pub fn tagged_header(tag: u8) -> Result<(storage::Shared, bool), storage::AllocError> {
    let payload: Option<&[u8]> = match tag {
        b't' => Some(&[2]),
        b'b' => Some(&[0x80]),
        b'B' => Some(&[0xff]),
        b'U' | b's' | b'u' => Some(&[0xff, 0xfe]),
        b'I' | b'i' => Some(&[0xff, 0xff, 0xff, 0xfe]),
        b'L' | b'l' | b'T' => Some(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]),
        b'f' => Some(&[0x3f, 0xc0, 0, 0]),
        b'd' => Some(&[0x3f, 0xf8, 0, 0, 0, 0, 0, 0]),
        b'D' => Some(&[2, 0xff, 0xff, 0xff, 0xfe]),
        b'S' | b'x' => Some(&[0, 0, 0, 2, 0xff, 0]),
        b'A' => Some(&[0, 0, 0, 3, b't', 2, b'V']),
        b'F' => Some(&[0, 0, 0, 4, 1, b'n', b't', 2]),
        b'V' => Some(&[]),
        _ => None,
    };
    let mut table = vec![0, 0, 0, 0, 1, b'k', tag];
    table.extend_from_slice(payload.unwrap_or_default());
    table.extend_from_slice(&[1, b'z', b'V']);
    let len = (table.len() - 4) as u32;
    table[..4].copy_from_slice(&len.to_be_bytes());
    Ok((header(1 << 13, &table)?, payload.is_some()))
}

pub const NESTED_INVALID_TAG: &[u8] = &[
    0, 0, 0, 15, 1, b'n', b'F', 0, 0, 0, 8, 1, b'a', b'A', 0, 0, 0, 1, b'?',
];

pub fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
