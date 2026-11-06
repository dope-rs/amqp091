use crate::common::amqp::io::{Read, Write};

use crate::common::amqp::wire::Frame;
use crate::common::amqp::wire::encoded::Encoded;
use crate::common::amqp::wire::header::Header;
use crate::common::amqp::wire::route::Route;
use crate::common::amqp::wire::segments;
use crate::common::amqp::{Error, wire};

/// One independently decoded AMQP method frame.
pub struct Method(pub(crate) u16, pub(crate) Vec<u8>);

impl Method {
    /// Encodes one method frame in a single owned buffer.
    pub fn try_encode(
        channel: u16,
        class: u16,
        method: u16,
        args: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let len = args.len() + 4;
        let mut encoded = Encoded::try_new(super::METHOD, channel, len)?;
        encoded.extend(&class.to_be_bytes());
        encoded.extend(&method.to_be_bytes());
        encoded.extend(args);
        encoded.finish()
    }

    /// Writes one method frame from borrowed segments without allocation.
    pub fn write(
        sink: &mut impl Write,
        channel: u16,
        class: u16,
        method: u16,
        args: &[u8],
    ) -> Result<(), Error> {
        let len = args.len() + 4;
        let header = Header::new(wire::METHOD, channel, len)?;
        let class = class.to_be_bytes();
        let method = method.to_be_bytes();
        let route = [class[0], class[1], method[0], method[1]];
        let tail = [wire::END];
        segments::write([header.bytes(), &route, args, &tail], sink)
    }

    pub(crate) fn write_parts(
        sink: &mut impl Write,
        channel: u16,
        class: u16,
        method: u16,
        args: &[&[u8]],
    ) -> Result<(), Error> {
        if args.len() > 5 {
            return Err(Error::Parts(args.len()));
        }
        let mut len = 4_usize;
        for arg in args {
            let Some(next) = len.checked_add(arg.len()) else {
                return Err(Error::Capacity { length: arg.len() });
            };
            len = next;
        }
        let header = Header::new(wire::METHOD, channel, len)?;
        let class = class.to_be_bytes();
        let method = method.to_be_bytes();
        let route = [class[0], class[1], method[0], method[1]];
        let tail = [wire::END];
        let mut parts: [&[u8]; 8] = [&[]; 8];
        parts[0] = header.bytes();
        parts[1] = &route;
        let end = args.len() + 2;
        parts[2..end].copy_from_slice(args);
        parts[7] = &tail;
        segments::write(parts, sink)
    }

    /// Reads one complete method frame and owns its payload once.
    pub fn read(source: &mut impl Read) -> Result<Self, Error> {
        Self::try_from(Frame::read(source)?)
    }

    pub const fn channel(&self) -> u16 {
        self.0
    }

    pub fn payload(&self) -> &[u8] {
        &self.1
    }

    pub fn arguments(&self) -> &[u8] {
        &self.1[4..]
    }

    pub fn class(&self) -> u16 {
        u16::from_be_bytes([self.1[0], self.1[1]])
    }

    pub fn method(&self) -> u16 {
        u16::from_be_bytes([self.1[2], self.1[3]])
    }

    /// Checks the exact method route without allocating on success.
    pub fn check(&self, channel: u16, class: u16, method: u16) -> Result<(), Error> {
        let expected = Route::new(channel, class, method);
        let actual = Route::new(self.channel(), self.class(), self.method());
        if actual == expected {
            Ok(())
        } else {
            Err(Error::route(expected, actual))
        }
    }
}

impl TryFrom<Frame> for Method {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        if frame.frame_type() != super::METHOD {
            return Err(Error::FrameType(frame.frame_type()));
        }
        if frame.payload().len() < 4 {
            return Err(Error::MethodLength(frame.payload().len()));
        }
        Ok(Self(frame.1, frame.2))
    }
}
