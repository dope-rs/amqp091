use dope_net::link::egress::stage::Stage;

use super::FrameBuffer;

pub trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, src: &[u8]);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn as_mut_slice(&mut self) -> &mut [u8];
}

impl Sink for FrameBuffer {
    fn push(&mut self, byte: u8) {
        self.try_push(byte)
            .unwrap_or_else(|_| std::process::abort());
    }
    fn extend_from_slice(&mut self, src: &[u8]) {
        self.try_extend_from_slice(src)
            .unwrap_or_else(|_| std::process::abort());
    }
    fn len(&self) -> usize {
        FrameBuffer::len(self)
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        FrameBuffer::as_mut_slice(self)
    }
}

impl<B> Sink for Stage<'_, '_, B> {
    fn push(&mut self, byte: u8) {
        Stage::push(self, byte);
    }
    fn extend_from_slice(&mut self, src: &[u8]) {
        Stage::extend_from_slice(self, src);
    }
    fn len(&self) -> usize {
        Stage::len(self)
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        Stage::as_mut_slice(self)
    }
}
