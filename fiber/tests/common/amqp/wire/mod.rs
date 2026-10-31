//! Independent AMQP 0-9-1 wire oracle for black-box tests.

use std::str;

use crate::common::amqp::Error;
pub(crate) mod encoded;
mod frame;
pub use frame::Frame;
pub(crate) mod header;
pub(crate) mod incoming;
pub(crate) mod input;
mod method;
pub use method::Method;
pub(crate) mod route;
pub(crate) mod segments;

const END: u8 = 0xce;
const METHOD: u8 = 1;

/// Decodes one independently observed short string without trusting the peer's length.
pub(crate) fn short(payload: &[u8], offset: usize) -> Result<(&str, usize), Error> {
    let missing = || Error::Truncated {
        offset,
        length: payload.len(),
    };
    let (&length, tail) = payload
        .get(offset..)
        .and_then(|tail| tail.split_first())
        .ok_or_else(missing)?;
    let (value, tail) = tail
        .split_at_checked(usize::from(length))
        .ok_or_else(missing)?;
    Ok((
        str::from_utf8(value).map_err(Error::Utf8)?,
        payload.len() - tail.len(),
    ))
}

pub(crate) fn check(actual: &[u8], expected: &[u8]) -> Result<(), Error> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Payload {
            expected: expected.to_vec(),
            actual: actual.to_vec(),
        })
    }
}

pub(crate) fn put_short(output: &mut Vec<u8>, value: impl AsRef<[u8]>) -> Result<(), Error> {
    let value = value.as_ref();
    let length = u8::try_from(value.len()).map_err(Error::Length)?;
    output.push(length);
    output.extend_from_slice(value);
    Ok(())
}
