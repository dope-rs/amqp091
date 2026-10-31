use std::io;
use std::io::Read;

use crate::common::amqp::Error;
use crate::common::amqp::wire::encoded::Encoded;
use crate::common::amqp::wire::header::Header;

/// One independently decoded AMQP frame.
pub struct Frame(pub(crate) u8, pub(crate) u16, pub(crate) Vec<u8>);

impl Frame {
    /// Encodes one frame in a single owned buffer.
    pub fn try_encode(kind: u8, channel: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let mut encoded = Encoded::try_new(kind, channel, payload.len())?;
        encoded.extend(payload);
        encoded.finish()
    }

    pub(crate) const fn frame_type(&self) -> u8 {
        self.0
    }

    pub fn payload(&self) -> &[u8] {
        &self.2
    }

    pub(crate) fn check(&self, kind: u8, channel: u16) -> Result<(), Error> {
        let expected = (kind, channel);
        let actual = (self.0, self.1);
        if actual == expected {
            Ok(())
        } else {
            Err(Error::Frame { expected, actual })
        }
    }

    pub(crate) fn read_body(
        source: &mut impl Read,
        channel: u16,
        expected: &[u8],
    ) -> Result<(), Error> {
        let mut received = Vec::with_capacity(expected.len());
        while received.len() < expected.len() {
            let frame = Self::read(source).map_err(Error::Io)?;
            frame.check(3, channel)?;
            received.extend_from_slice(frame.payload());
        }
        super::check(&received, expected)
    }

    /// Reads one complete frame and owns its payload once.
    pub fn read(source: &mut impl Read) -> io::Result<Self> {
        let header = Header::read(source)?;
        let length = header.length() as usize;
        let mut payload = vec![0_u8; length];
        source.read_exact(&mut payload)?;
        let mut end = [0_u8; 1];
        source.read_exact(&mut end)?;
        if end[0] != super::END {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid AMQP frame end",
            ));
        }
        Ok(Self(header.frame_type(), header.channel(), payload))
    }
}
