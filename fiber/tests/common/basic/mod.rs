use std::time::Duration;

use crate::common::amqp::app::Policy;
use crate::common::amqp::wire::Frame;
use crate::common::amqp::{Error, Tune};

pub const POLICY: Policy = Policy::new(1, 8, Duration::from_secs(10));
pub const TUNE: Tune = Tune::new(2, 4096, 0);
pub const EXCHANGE: &str = "official-exchange";
pub const ROUTING_KEY: &str = "official.route";
pub const BODY: &[u8] = b"fragmented-body";

pub fn content_header(channel: u16, size: u64) -> Result<Vec<u8>, Error> {
    content_header_with(channel, size, 0, &[])
}

pub(crate) fn content_header_with(
    channel: u16,
    size: u64,
    flags: u16,
    props: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut header = Vec::with_capacity(14 + props.len());
    header.extend_from_slice(&60_u16.to_be_bytes());
    header.extend_from_slice(&0_u16.to_be_bytes());
    header.extend_from_slice(&size.to_be_bytes());
    header.extend_from_slice(&flags.to_be_bytes());
    header.extend_from_slice(props);
    Frame::try_encode(2, channel, &header)
}

pub mod delivery;
pub mod returned;
