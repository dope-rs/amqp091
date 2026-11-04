use crate::common::amqp::io::{Read, Write};

use crate::common::amqp::Error;
use crate::common::amqp::wire::{self, Frame, Method};
use crate::common::basic;

pub const CONTENT_TYPE: &str = "application/x-amqp-return";
pub const REPLY_CODE: u16 = 312;
pub const REPLY_TEXT: &str = "NO_ROUTE";

pub fn method(channel: u16, exchange: &str, key: &str) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(REPLY_TEXT.len() + exchange.len() + key.len() + 5);
    args.extend_from_slice(&REPLY_CODE.to_be_bytes());
    wire::put_short(&mut args, REPLY_TEXT)?;
    wire::put_short(&mut args, exchange)?;
    wire::put_short(&mut args, key)?;
    Method::try_encode(channel, 60, 50, &args)
}

pub fn content(
    channel: u16,
    exchange: &str,
    key: &str,
    chunks: &[&[u8]],
    mime: Option<&str>,
) -> Result<Vec<u8>, Error> {
    let mut props = Vec::new();
    let flags = match mime {
        Some(value) => {
            wire::put_short(&mut props, value)?;
            1 << 15
        }
        None => 0,
    };
    frames(channel, exchange, key, chunks, flags, &props)
}

pub fn frames(
    channel: u16,
    exchange: &str,
    key: &str,
    chunks: &[&[u8]],
    flags: u16,
    props: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut frames = method(channel, exchange, key)?;
    let size = chunks.iter().map(|chunk| chunk.len() as u64).sum();
    frames.extend_from_slice(&basic::content_header_with(channel, size, flags, props)?);
    for chunk in chunks {
        frames.extend_from_slice(&Frame::try_encode(3, channel, chunk)?);
    }
    Ok(frames)
}

pub fn check_publish(
    stream: &mut impl Read,
    channel: u16,
    exchange: &str,
    key: &str,
    body: &[u8],
    mime: Option<&str>,
) -> Result<(), Error> {
    let method = Method::read(stream)?;
    method.check(channel, 60, 40)?;
    let (actual, offset) = wire::short(&method.1, 6)?;
    wire::check(actual.as_bytes(), exchange.as_bytes())?;
    let (actual, offset) = wire::short(&method.1, offset)?;
    wire::check(actual.as_bytes(), key.as_bytes())?;
    let flags = method.1.get(offset).ok_or(Error::Truncated {
        offset,
        length: method.1.len(),
    })?;
    wire::check(&[flags & 3], &[1])?;

    let frame = Frame::read(stream)?;
    frame.check(2, channel)?;
    let (header, tail) = frame
        .payload()
        .split_first_chunk::<12>()
        .ok_or(Error::Truncated {
            offset: 12,
            length: frame.payload().len(),
        })?;
    wire::check(&header[..4], &[0, 60, 0, 0])?;
    wire::check(&header[4..12], &(body.len() as u64).to_be_bytes())?;
    let mut props = Vec::new();
    props.extend_from_slice(&(if mime.is_some() { 1_u16 << 15 } else { 0 }).to_be_bytes());
    if let Some(expected) = mime {
        wire::put_short(&mut props, expected)?;
    }
    wire::check(tail, &props)?;
    Frame::read_body(stream, channel, body)
}

pub fn open_channel(stream: &mut (impl Read + Write)) -> Result<u16, Error> {
    let method = Method::read(stream)?;
    let channel = method.channel();
    method.check(channel, 20, 10)?;
    Method::write(stream, channel, 20, 11, &[0, 0, 0, 0])?;
    Ok(channel)
}

pub fn close_connection(stream: &mut (impl Read + Write)) -> Result<(), Error> {
    Method::read(stream)?.check(0, 10, 50)?;
    Method::write(stream, 0, 10, 51, &[])?;
    Ok(())
}

pub fn check_connection_close(stream: &mut (impl Read + Write), code: u16) -> Result<(), Error> {
    let close = Method::read(stream)?;
    close.check(0, 10, 50)?;
    let prefix = close.1.first_chunk::<6>().ok_or(Error::Truncated {
        offset: 6,
        length: close.1.len(),
    })?;
    wire::check(&prefix[4..6], &code.to_be_bytes())?;
    Method::write(stream, 0, 10, 51, &[])?;
    Ok(())
}
