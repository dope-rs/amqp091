use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::manifold::timing;
use dope_amqp091::config;

use crate::common::amqp::app::{Endpoint, Policy, Scope, error};
use crate::common::amqp::wire::{self, Frame, Method};
use crate::common::amqp::{Error, Tune};
use crate::common::basic;
const POLICY: Policy = Policy::new(1, 8, Duration::from_secs(10));
pub const TUNE: Tune = Tune::new(1, 4096, 0);

#[pin_project::pin_project]
#[derive(dope::Application)]
pub struct Split<H> {
    #[pin]
    #[app]
    primary: H,
    #[pin]
    #[app(optional)]
    idle: Option<H>,
}

impl<H> Split<H> {
    pub const fn new(primary: H) -> Self {
        Self {
            primary,
            idle: None,
        }
    }

    pub fn primary(self: Pin<&Self>) -> Pin<&H> {
        self.project_ref().primary
    }
}

pub fn with_client(
    address: SocketAddr,
    config: config::Config,
    test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
        &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, timing::Throughput>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<(), error::Error<'owner, ()>>),
) {
    Endpoint::new(address, config)
        .try_check::<timing::Throughput, _, _>(POLICY, test, recover, finish)
}

pub fn frame(channel: u16, tag: &str) -> Result<Vec<u8>, Error> {
    let mut args = Vec::with_capacity(tag.len() + 2);
    wire::put_short(&mut args, tag)?;
    args.push(1);
    Method::try_encode(channel, 60, 30, &args)
}

pub fn delivery_frames(channel: u16, tag: &str) -> Result<Vec<u8>, Error> {
    delivery_frames_with_tag(channel, tag, 1)
}

pub fn delivery_frames_with_tag(channel: u16, tag: &str, delivery: u64) -> Result<Vec<u8>, Error> {
    let mut frames = basic::delivery::method(channel, tag, delivery)?;

    frames.extend_from_slice(&basic::content_header(channel, 1)?);
    frames.extend_from_slice(&Frame::try_encode(3, channel, b"x")?);
    Ok(frames)
}

pub use crate::common::basic::delivery::consume_ok as consume_ok_arguments;

pub fn check(frame: &Method, tag: &str) -> Result<(), Error> {
    check_on(frame, 1, tag)
}

pub fn check_on(frame: &Method, channel: u16, tag: &str) -> Result<(), Error> {
    frame.check(channel, 60, 30)?;
    let (actual, offset) = wire::short(&frame.1, 4)?;
    wire::check(actual.as_bytes(), tag.as_bytes())?;
    let flag = frame.1.get(offset).ok_or(Error::Truncated {
        offset,
        length: frame.1.len(),
    })?;
    wire::check(&[*flag], &[0])
}

pub fn check_client_consume(frame: &Method, tag: &str) -> Result<(), Error> {
    check_client_consume_on(frame, 1, tag)
}

pub fn check_client_consume_on(frame: &Method, channel: u16, tag: &str) -> Result<(), Error> {
    frame.check(channel, 60, 20)?;
    let (_, offset) = wire::short(&frame.1, 6)?;
    let (actual, _) = wire::short(&frame.1, offset)?;
    wire::check(actual.as_bytes(), tag.as_bytes())
}
