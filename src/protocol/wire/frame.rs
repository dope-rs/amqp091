use std::{cell, mem, num};

use codec::staging::Buffered;
use dope::core::io::transfer;
use dope::manifold::client::session::codec;
use o3::buffer;
use o3::buffer::hold::scope::Deferred;
use o3::buffer::resident::staging;
use o3::buffer::write::consume::scope;

use crate::protocol::wire::Write;

pub(crate) struct Type;

impl Type {
    pub(crate) const METHOD: u8 = 1;
    pub(crate) const HEADER: u8 = 2;
    pub(crate) const BODY: u8 = 3;
    pub(crate) const HEARTBEAT: u8 = 8;
    pub(crate) const END: u8 = 0xCE;
}

const PREFACE: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];

pub(crate) const HEADER_LEN: usize = 7;
const END_LEN: usize = 1;
pub(crate) const OVERHEAD: usize = HEADER_LEN + END_LEN;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Channel(num::NonZero<u16>);

impl Channel {
    pub(crate) const fn new(channel: u16) -> Option<Self> {
        match num::NonZero::<u16>::new(channel) {
            Some(channel) => Some(Self(channel)),
            None => None,
        }
    }

    pub(crate) const fn get(self) -> u16 {
        self.0.get()
    }
}

pub enum Inbound<'input, 'd, R: buffer::Cursor> {
    Method {
        channel: u16,
        payload: Deferred<'input, 'd, R>,
    },
    Header {
        channel: Channel,
        payload: Deferred<'input, 'd, R>,
    },
    Body {
        channel: Channel,
        payload: Deferred<'input, 'd, R>,
    },
    Heartbeat,
    Fault(Fault),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    FrameTooLarge { size: u64, max: u32 },
    ContentChannel { frame_type: u8, channel: u16 },
    HeartbeatChannel { channel: u16 },
    HeartbeatPayload { len: u32 },
    Violation(Violation),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Violation {
    UndefinedType(u8),
    InvalidEnd(u8),
}

#[derive(Clone, Copy)]
enum Kind {
    Method,
    Header(Channel),
    Body(Channel),
}

pub struct Codec {
    max: cell::Cell<u32>,
}

impl Codec {
    pub fn new(max: u32) -> Self {
        Self {
            max: cell::Cell::new(max),
        }
    }

    pub(crate) fn set_max(&self, max: u32) {
        self.max.set(max);
    }

    fn fault<'id, 'input, 'd, R: buffer::Cursor>(
        fault: Fault,
        consumed: scope::End<'id, { transfer::MAX_BYTES as u32 }>,
    ) -> codec::Parse<'id, Inbound<'input, 'd, R>> {
        codec::Parse::Item {
            head: Inbound::Fault(fault),
            consumed,
        }
    }
}

impl codec::Codec for Codec {
    type Head<'input, 'd, R: buffer::Cursor>
        = Inbound<'input, 'd, R>
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        const WARM: usize = 16 * 1024;
        let limit = self.max.get() as usize;
        staging::Plan::new(limit.min(WARM), limit)
    }
}

impl codec::Stateless for Codec {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        buf: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        let Some(bytes) = buf.as_slice().first_chunk::<HEADER_LEN>() else {
            return Ok(match buf.need_to(HEADER_LEN) {
                Some(need) => codec::Parse::More(need),
                None => codec::Parse::CapacityExhausted,
            });
        };
        let header = buf.try_consumed(num::NonZero::<usize>::MIN.saturating_add(HEADER_LEN - 1))?;
        let tag = bytes[0];
        let channel = u16::from_be_bytes([bytes[1], bytes[2]]);
        let wire = u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]);
        let wire_len = u64::from(wire) + OVERHEAD as u64;
        if wire_len > u64::from(self.max.get()) {
            return Ok(Self::fault(
                Fault::FrameTooLarge {
                    size: wire_len,
                    max: self.max.get(),
                },
                header,
            ));
        }
        let kind = match tag {
            Type::METHOD => Some(Kind::Method),
            Type::HEADER => {
                let Some(channel) = Channel::new(channel) else {
                    return Ok(Self::fault(
                        Fault::ContentChannel {
                            frame_type: tag,
                            channel,
                        },
                        header,
                    ));
                };
                Some(Kind::Header(channel))
            }
            Type::BODY => {
                let Some(channel) = Channel::new(channel) else {
                    return Ok(Self::fault(
                        Fault::ContentChannel {
                            frame_type: tag,
                            channel,
                        },
                        header,
                    ));
                };
                Some(Kind::Body(channel))
            }
            Type::HEARTBEAT => {
                if channel != 0 {
                    return Ok(Self::fault(Fault::HeartbeatChannel { channel }, header));
                }
                if wire != 0 {
                    return Ok(Self::fault(Fault::HeartbeatPayload { len: wire }, header));
                }
                None
            }
            other => {
                return Ok(Self::fault(
                    Fault::Violation(Violation::UndefinedType(other)),
                    header,
                ));
            }
        };
        let len = wire as usize;
        let total = wire_len as usize;
        let Some(end_byte) = buf.as_slice().get(total - END_LEN) else {
            return Ok(match buf.need_to(total) {
                Some(need) => codec::Parse::More(need),
                None => codec::Parse::CapacityExhausted,
            });
        };
        let consumed = buf.try_consumed(num::NonZero::<usize>::MIN.saturating_add(total - 1))?;
        let end_byte = *end_byte;
        if end_byte != Type::END {
            return Ok(Self::fault(
                Fault::Violation(Violation::InvalidEnd(end_byte)),
                consumed,
            ));
        }
        let Some(kind) = kind else {
            return Ok(codec::Parse::Item {
                head: Inbound::Heartbeat,
                consumed,
            });
        };
        let payload = buf
            .try_plan(HEADER_LEN..HEADER_LEN + len)
            .map_err(scope::Error::Range)?;
        let head = match kind {
            Kind::Method => Inbound::Method { channel, payload },
            Kind::Header(channel) => Inbound::Header { channel, payload },
            Kind::Body(channel) => Inbound::Body { channel, payload },
        };
        Ok(codec::Parse::Item { head, consumed })
    }
}

pub(crate) struct Encoder<'a, S: ?Sized> {
    out: &'a mut S,
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Body<'a>(&'a [u8]);

pub(crate) struct Payload<'a> {
    bytes: &'a [u8],
    len: u32,
}

impl<'a> Body<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.is_empty() {
            None
        } else {
            Some(Self(bytes))
        }
    }

    pub(crate) fn split(self, max: num::NonZero<u32>) -> (Payload<'a>, Option<Self>) {
        let cap = max.get() as usize;
        if self.0.len() >= cap {
            let (bytes, tail) = self.0.split_at(cap);
            return (
                Payload {
                    bytes,
                    len: max.get(),
                },
                Self::new(tail),
            );
        }
        (
            Payload {
                bytes: self.0,
                len: self.0.len() as u32,
            },
            None,
        )
    }
}

impl<'a, S: Write + ?Sized> Encoder<'a, S> {
    pub(crate) fn new(out: &'a mut S) -> Self {
        Self { out }
    }

    pub(crate) fn body(self, channel: u16, payload: Payload<'_>) {
        let out = self.out;
        out.push(Type::BODY);
        out.extend(&channel.to_be_bytes());
        out.extend(&payload.len.to_be_bytes());
        out.extend(payload.bytes);
        out.push(Type::END);
    }

    pub(crate) fn heartbeat(self) {
        self.out
            .extend(&[Type::HEARTBEAT, 0, 0, 0, 0, 0, 0, Type::END]);
    }

    pub(crate) fn preface(self) {
        self.out.extend(&PREFACE);
    }
}

impl<S: Write + AsMut<[u8]> + ?Sized> Encoder<'_, S> {
    pub(crate) fn method_with<R, F: FnOnce(&mut S) -> R>(
        self,
        channel: u16,
        id: [u8; 4],
        args: F,
    ) -> R {
        self.write(Type::METHOD, channel, |out| {
            out.extend(&id);
            args(out)
        })
    }

    pub(crate) fn write<R, F: FnOnce(&mut S) -> R>(self, kind: u8, channel: u16, body: F) -> R {
        let out = self.out;
        let head = out.len();
        for _ in 0..HEADER_LEN {
            out.push(0);
        }
        let start = out.len();
        let value = body(out);
        if out.status().is_err() {
            return value;
        }
        let len = (out.len() - start) as u32;
        let slice = out.as_mut();
        let header = &mut slice[head..head + HEADER_LEN];
        header[0] = kind;
        header[1..3].copy_from_slice(&channel.to_be_bytes());
        header[3..7].copy_from_slice(&len.to_be_bytes());
        out.push(Type::END);
        value
    }
}

const _: () = assert!(mem::size_of::<Channel>() == mem::size_of::<u16>());
const _: () = assert!(mem::align_of::<Channel>() == mem::align_of::<u16>());
const _: () = assert!(mem::size_of::<Body<'_>>() == mem::size_of::<&[u8]>());
const _: () = assert!(mem::size_of::<Option<Body<'_>>>() == mem::size_of::<Body<'_>>());
const _: () = assert!(mem::size_of::<Violation>() == mem::size_of::<u16>());
const _: () = assert!(usize::BITS >= u32::BITS);
