//! Public AMQP client behavior exercised through the fiber API.

use crate::common::amqp::io::{Read, Write};
use dope_test::harness::peer;
use std::net::SocketAddr;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::manifold::timing;
use dope_amqp091::config;
use dope_test::harness::peer::Stream;

use crate::common::amqp::app::{Endpoint, Policy, Scope, error};
use crate::common::amqp::wire::{self, Frame, Method};
use crate::common::amqp::{Error, Tune};
use crate::common::basic;

pub static STREAMED_BODY: [u8; 64 * 1024] = [0; 64 * 1024];
pub static STAGING_HELD: [u8; 96 * 1024] = [0x5a; 96 * 1024];
pub static STAGING_NEXT: [u8; 96 * 1024] = [0xa5; 96 * 1024];
pub static STAGING_BOUNDARY: [u8; 131_072 - 22] = [0x69; 131_072 - 22];
pub(crate) const POLICY: Policy = Policy::new(1, 16, Duration::from_secs(30));
pub const TUNE: Tune = Tune::new(16, 131_072, 0);

pub fn with(
    address: SocketAddr,
    config: config::Config,
    test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
        &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, timing::Throughput>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<(), error::Error<'owner, ()>>),
) {
    Endpoint::new(address, config).try_check::<timing::Throughput, _>(POLICY, test, recover, finish)
}

pub fn queue_declare_ok_frame(channel: u16, name: &[u8]) -> Result<Vec<u8>, Error> {
    let mut response = Vec::new();
    wire::put_short(&mut response, name)?;
    response.extend_from_slice(&0u32.to_be_bytes());
    response.extend_from_slice(&0u32.to_be_bytes());
    Method::try_encode(channel, 50, 11, &response)
}

pub fn returned_body(channel: u16, chunks: &[&[u8]]) -> Result<Vec<u8>, Error> {
    basic::returned::frames(channel, "", "", chunks, 0, &[])
}

pub fn staging_header(channel: u16) -> Result<Vec<u8>, Error> {
    let bytes = u32::try_from(STAGING_BOUNDARY.len()).map_err(Error::Length)?;
    let mut properties = (bytes - 4).to_be_bytes().to_vec();
    properties.extend_from_slice(&[1, b'x', b'S']);
    properties.extend_from_slice(&(bytes - 11).to_be_bytes());
    properties.resize(bytes as usize, b'x');
    basic::content_header_with(channel, 0, 1 << 13, &properties)
}

pub const RETURNED_PROPERTY_BYTES: usize = 20;

pub fn returned_properties(
    channel: u16,
    value: &[u8; 3],
    chunks: &[&[u8]],
) -> Result<Vec<u8>, Error> {
    let [a, b, c] = *value;
    let property: [u8; RETURNED_PROPERTY_BYTES] = [
        3, a, b, c, 0, 0, 0, 10, 1, b'x', b'S', 0, 0, 0, 3, a, b, c, 2, 9,
    ];
    basic::returned::frames(
        channel,
        "",
        "",
        chunks,
        (1 << 15) | (1 << 13) | (1 << 12) | (1 << 11),
        &property,
    )
}

pub fn check_no_frame(stream: &Stream<'_>, context: &str) -> Result<(), Error> {
    stream.read_wait(Some(Duration::from_millis(250)))?;
    let mut byte = [0];
    let result = stream.read(&mut byte);
    match result {
        Err(error)
            if matches!(error, peer::Error::TimedOut)
                || matches!(error, peer::Error::Syscall(error) if error.is_block()) => {}
        Err(error) => return Err(Error::Io(error)),
        Ok(bytes) => {
            return Err(Error::UnexpectedRead {
                context: context.to_owned(),
                bytes,
            });
        }
    }
    stream.read_wait(Some(Duration::from_secs(10)))?;
    Ok(())
}

pub fn queue_name(frame: &Method) -> Result<&str, Error> {
    wire::short(&frame.1, 6).map(|(name, _)| name)
}

pub fn check_queue_bind_target(frame: &Method, queue: &str, route: &str) -> Result<(), Error> {
    frame.check(1, 50, 20)?;
    let (actual, offset) = wire::short(&frame.1, 6)?;
    wire::check(actual.as_bytes(), queue.as_bytes())?;
    let (_, offset) = wire::short(&frame.1, offset)?;
    let (actual, _) = wire::short(&frame.1, offset)?;
    wire::check(actual.as_bytes(), route.as_bytes())?;
    Ok(())
}

pub fn check_consume_target(frame: &Method, queue: &str, tag: &str) -> Result<(), Error> {
    frame.check(1, 60, 20)?;
    let (actual, offset) = wire::short(&frame.1, 6)?;
    wire::check(actual.as_bytes(), queue.as_bytes())?;
    let (actual, _) = wire::short(&frame.1, offset)?;
    wire::check(actual.as_bytes(), tag.as_bytes())?;
    Ok(())
}

pub fn check_publish(
    stream: &mut impl Read,
    channel: u16,
    route: &str,
    body: &[u8],
) -> Result<(), Error> {
    let publish = Method::read(stream)?;
    publish.check(channel, 60, 40)?;
    let (_, offset) = wire::short(&publish.1, 6)?;
    let (actual, _) = wire::short(&publish.1, offset)?;
    wire::check(actual.as_bytes(), route.as_bytes())?;

    let frame = Frame::read(stream)?;
    frame.check(2, channel)?;
    let header = frame
        .payload()
        .first_chunk::<12>()
        .ok_or(Error::Truncated {
            offset: 12,
            length: frame.payload().len(),
        })?;
    wire::check(&header[..2], &60u16.to_be_bytes())?;
    wire::check(&header[4..12], &(body.len() as u64).to_be_bytes())?;
    Frame::read_body(stream, channel, body)
}

pub fn confirm_select(stream: &mut (impl Read + Write), channel: u16) -> Result<(), Error> {
    Method::read(stream)?.check(channel, 85, 10)?;
    Method::write(stream, channel, 85, 11, &[])
}

pub fn confirm_ack(stream: &mut impl Write, channel: u16, tag: u64) -> Result<(), Error> {
    Method::write_parts(stream, channel, 60, 80, &[&tag.to_be_bytes(), &[0]])
}

pub fn confirm_ack_all(stream: &mut impl Write, channel: u16) -> Result<(), Error> {
    Method::write_parts(stream, channel, 60, 80, &[&0_u64.to_be_bytes(), &[1]])
}

pub fn confirm_nack_all(stream: &mut impl Write, channel: u16, requeue: bool) -> Result<(), Error> {
    let flags = [1 | (u8::from(requeue) << 1)];
    Method::write_parts(stream, channel, 60, 120, &[&0_u64.to_be_bytes(), &flags])
}

pub mod channel;

mod redial;
pub use redial::Redial;

pub mod broker;

pub fn close_reason(text: &str) -> Result<Vec<u8>, Error> {
    let len = u8::try_from(text.len()).map_err(Error::Length)?;
    let mut args = Vec::from(320_u16.to_be_bytes());
    args.push(len);
    args.extend_from_slice(text.as_bytes());
    args.extend_from_slice(&[0; 4]);
    Ok(args)
}
