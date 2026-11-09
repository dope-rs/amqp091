use std::time::Duration;

use dope_test::harness::peer::Stream;

use crate::common::amqp::wire::{self, Frame, Method};
use crate::common::amqp::{Error, Peer};
use crate::common::basic;
use crate::common::recovery::TUNE;

pub fn queue_declare_ok(
    channel: u16,
    queue: &str,
    messages: u32,
    users: u32,
) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(9 + queue.len());
    wire::put_short(&mut args, queue)?;
    args.extend_from_slice(&messages.to_be_bytes());
    args.extend_from_slice(&users.to_be_bytes());
    Method::try_encode(channel, 50, 11, &args)
}

pub fn consume_ok(channel: u16, tag: &str) -> Result<Vec<u8>, Error> {
    Method::try_encode(channel, 60, 21, &basic::delivery::consume_ok(tag)?)
}

pub struct Delivery<'a> {
    pub(crate) channel: u16,
    pub(crate) consumer_tag: &'a str,
    pub(crate) delivery_tag: u64,
    pub(crate) redelivered: bool,
    pub(crate) exchange: &'a str,
    pub(crate) routing_key: &'a str,
    pub(crate) body: &'a [u8],
}

pub fn delivery_frames(delivery: Delivery<'_>) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(
        13 + delivery.consumer_tag.len() + delivery.exchange.len() + delivery.routing_key.len(),
    );
    wire::put_short(&mut args, delivery.consumer_tag)?;
    args.extend_from_slice(&delivery.delivery_tag.to_be_bytes());
    args.push(u8::from(delivery.redelivered));
    wire::put_short(&mut args, delivery.exchange)?;
    wire::put_short(&mut args, delivery.routing_key)?;

    let mut frames = Method::try_encode(delivery.channel, 60, 60, &args)?;
    frames.extend_from_slice(&basic::content_header(
        delivery.channel,
        delivery.body.len() as u64,
    )?);
    if !delivery.body.is_empty() {
        frames.extend_from_slice(&Frame::try_encode(3, delivery.channel, delivery.body)?);
    }
    Ok(frames)
}

pub fn channel_close(
    channel: u16,
    code: u16,
    text: &str,
    class: u16,
    method: u16,
) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(7 + text.len());
    args.extend_from_slice(&code.to_be_bytes());
    wire::put_short(&mut args, text)?;
    args.extend_from_slice(&class.to_be_bytes());
    args.extend_from_slice(&method.to_be_bytes());
    Method::try_encode(channel, 20, 40, &args)
}

pub fn open_named(
    mut stream: &Stream<'_>,
    channel: u16,
    queue: &str,
    tag: &str,
) -> Result<(), Error> {
    Method::read(&mut stream)?.check(channel, 20, 10)?;
    stream.write_all(&Method::try_encode(channel, 20, 11, &[0, 0, 0, 0])?)?;
    Method::read(&mut stream)?.check(channel, 50, 10)?;
    stream.write_all(&queue_declare_ok(channel, queue, 0, 0)?)?;
    Method::read(&mut stream)?.check(channel, 60, 20)?;
    stream.write_all(&consume_ok(channel, tag)?)?;
    Ok(())
}

pub fn tagged(tag: u64, body: &[u8]) -> Result<Vec<u8>, Error> {
    delivery_frames(Delivery {
        channel: 1,
        consumer_tag: "tag",
        delivery_tag: tag,
        redelivered: false,
        exchange: "",
        routing_key: "rk",
        body,
    })
}

pub fn open_tagged(mut stream: &Stream<'_>) -> Result<(), Error> {
    let limit = Some(Duration::from_secs(5));
    stream.read_wait(limit)?;
    stream.write_wait(limit)?;
    Peer::new(&mut stream).open(TUNE)?;
    open_named(stream, 1, "q", "tag")
}

pub fn ack_tag(method: &Method) -> Result<u64, Error> {
    let prefix = method.1.first_chunk::<12>().ok_or(Error::Truncated {
        offset: 12,
        length: method.1.len(),
    })?;
    Ok(u64::from_be_bytes(
        prefix[4..12].try_into().map_err(Error::Slice)?,
    ))
}
