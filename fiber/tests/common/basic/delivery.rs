use dope_amqp091::message::ack;

use crate::common::amqp::Error;
use crate::common::amqp::wire::{self, Frame, Method};
use crate::common::basic;

pub fn consume_ok(tag: &str) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(tag.len() + 1);
    wire::put_short(&mut args, tag)?;
    Ok(args)
}

pub fn encode(tag: &str) -> Result<Vec<u8>, Error> {
    let mut args =
        Vec::with_capacity(tag.len() + basic::EXCHANGE.len() + basic::ROUTING_KEY.len() + 14);
    wire::put_short(&mut args, tag)?;
    args.extend_from_slice(&1_u64.to_be_bytes());
    args.push(0);
    wire::put_short(&mut args, basic::EXCHANGE)?;
    wire::put_short(&mut args, basic::ROUTING_KEY)?;
    let mut frames = Method::try_encode(1, 60, 60, &args)?;

    frames.extend_from_slice(&basic::content_header(1, basic::BODY.len() as u64)?);
    frames.extend_from_slice(&Frame::try_encode(3, 1, b"fragmented-")?);
    frames.extend_from_slice(&Frame::try_encode(3, 1, b"body")?);
    Ok(frames)
}

pub fn empty(tag: &str) -> Result<Vec<u8>, Error> {
    let mut frames = method(1, tag, 2)?;

    frames.extend_from_slice(&basic::content_header(1, 0)?);
    Ok(frames)
}

pub fn method(channel: u16, consumer: &str, tag: u64) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(consumer.len() + 14);
    wire::put_short(&mut args, consumer)?;
    args.extend_from_slice(&tag.to_be_bytes());
    args.extend_from_slice(&[0, 0, 0]);
    Method::try_encode(channel, 60, 60, &args)
}

pub fn byte(consumer: &str, tag: u64, byte: u8) -> Result<Vec<u8>, Error> {
    let mut frames = method(1, consumer, tag)?;
    frames.extend_from_slice(&basic::content_header(1, 1)?);
    frames.extend_from_slice(&Frame::try_encode(3, 1, &[byte])?);
    Ok(frames)
}

pub fn check_consume<M: ack::Mode>(
    method: &Method,
    channel: u16,
    queue: &str,
    tag: &str,
) -> Result<(), Error> {
    method.check(channel, 60, 20)?;
    let (actual, offset) = wire::short(&method.1, 6)?;
    wire::check(actual.as_bytes(), queue.as_bytes())?;
    let (actual, offset) = wire::short(&method.1, offset)?;
    wire::check(actual.as_bytes(), tag.as_bytes())?;
    let flags = method.1.get(offset).ok_or(Error::Truncated {
        offset,
        length: method.1.len(),
    })?;
    let expected = if M::NO_ACK { 0b10 } else { 0 };
    wire::check(&[*flags], &[expected])
}
