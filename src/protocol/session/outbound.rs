use super::super::*;

pub struct Outbound<'d> {
    buffer: Lease<'d>,
    overflowed: bool,
}

impl<'d> Outbound<'d> {
    pub(in crate::protocol) fn new(buffer: Lease<'d>) -> Self {
        Self {
            buffer,
            overflowed: false,
        }
    }

    pub(in crate::protocol) fn overflowed(&self) -> bool {
        self.overflowed
    }
}

impl AsRef<[u8]> for Outbound<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl Sink for Outbound<'_> {
    fn push(&mut self, byte: u8) {
        if self.buffer.try_push(byte).is_err() {
            self.overflowed = true;
        }
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        if self.buffer.try_extend_from_slice(src).is_err() {
            self.overflowed = true;
        }
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }
}
