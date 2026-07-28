use dope_net::link::egress::stage::Stage;
use o3::buffer::Block;

pub trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, src: &[u8]);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn as_mut_slice(&mut self) -> &mut [u8];
}

impl Sink for Block {
    fn push(&mut self, byte: u8) {
        Block::push(self, byte);
    }
    fn extend_from_slice(&mut self, src: &[u8]) {
        Block::extend_from_slice(self, src);
    }
    fn len(&self) -> usize {
        Block::len(self)
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        Block::as_mut_slice(self)
    }
}

impl<B> Sink for Stage<'_, B> {
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
