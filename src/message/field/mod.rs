pub use table::Table;
pub use value::Value;

use std::num;

use crate::failure;
use crate::failure::wire::field::Fault;
use crate::failure::wire::{self, Encode, Encoding};
use crate::protocol::wire::reader;

mod table;
mod value;
pub mod view;

const MAX_DEPTH: usize = 64;

#[inline]
pub(in crate::message) fn try_short<'a>(
    reader: &mut reader::Reader<'a>,
    offset: usize,
) -> Result<&'a str, failure::Error> {
    let value = reader
        .short()
        .map_err(|error| failure::Error::Field(Fault::Read(reader::Fault { offset, error })))?;
    if let Some(at) = value.bytes().position(|byte| byte == 0) {
        return Err(failure::Error::Protocol(wire::Fault::TextNul {
            kind: Encoding::Short,
            offset: offset + 1 + at,
        }));
    }
    Ok(value)
}

fn sum(
    kind: Encoding,
    left: u32,
    right: num::NonZero<u32>,
) -> Result<num::NonZero<u32>, failure::Error> {
    right.checked_add(left).ok_or_else(|| {
        failure::Error::Encoding(Encode::Sum {
            kind,
            left,
            right: right.get(),
            max: u32::MAX,
        })
    })
}
