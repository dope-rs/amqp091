use crate::common::amqp::Error;
use crate::common::amqp::io::Read;

pub(super) struct Header([u8; 7]);

impl Header {
    pub(super) fn new(kind: u8, channel: u16, length: usize) -> Result<Self, Error> {
        let length = u32::try_from(length).map_err(Error::Length)?;
        let mut bytes = [0; 7];
        bytes[0] = kind;
        bytes[1..3].copy_from_slice(&channel.to_be_bytes());
        bytes[3..7].copy_from_slice(&length.to_be_bytes());
        Ok(Self(bytes))
    }

    pub(super) fn read(source: &mut impl Read) -> Result<Self, Error> {
        let mut bytes = [0; 7];
        source.read_exact(&mut bytes)?;
        Ok(Self(bytes))
    }

    pub(super) const fn frame_type(&self) -> u8 {
        self.0[0]
    }

    pub(super) fn channel(&self) -> u16 {
        u16::from_be_bytes([self.0[1], self.0[2]])
    }

    pub(super) fn length(&self) -> u32 {
        u32::from_be_bytes([self.0[3], self.0[4], self.0[5], self.0[6]])
    }

    pub(super) const fn bytes(&self) -> &[u8; 7] {
        &self.0
    }
}
