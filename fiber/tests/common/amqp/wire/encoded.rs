use crate::common::amqp::Error;
pub(super) struct Encoded {
    bytes: Vec<u8>,
    payload_end: usize,
}

impl Encoded {
    pub(super) fn try_new(kind: u8, channel: u16, len: usize) -> Result<Self, Error> {
        let wire_len = u32::try_from(len).map_err(Error::Length)?;
        let capacity = len.checked_add(8).ok_or(Error::Capacity { length: len })?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(kind);
        bytes.extend_from_slice(&channel.to_be_bytes());
        bytes.extend_from_slice(&wire_len.to_be_bytes());
        Ok(Self {
            bytes,
            payload_end: capacity - 1,
        })
    }

    pub(super) fn extend(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(super) fn finish(mut self) -> Result<Vec<u8>, Error> {
        if self.bytes.len() != self.payload_end {
            return Err(Error::PayloadLength {
                expected: self.payload_end,
                actual: self.bytes.len(),
            });
        }
        self.bytes.push(super::END);
        Ok(self.bytes)
    }
}
