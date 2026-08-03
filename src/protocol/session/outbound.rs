use super::super::*;
use dope_net::link::egress::LeaseBuffer;

pub type Outbound<'d> = LeaseBuffer<'d>;

impl Sink for Outbound<'_> {
    fn push(&mut self, byte: u8) {
        let _ = self.try_push(byte);
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        let _ = self.try_extend_from_slice(src);
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}
