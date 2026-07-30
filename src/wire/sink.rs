use dope_net::link::egress::stage::Stage;
use o3::buffer::{CapacityError, Owned};

pub struct BoundedSink<const CAP: u32> {
    buffer: Owned<CAP>,
    error: Option<CapacityError>,
}

impl<const CAP: u32> BoundedSink<CAP> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Owned::new(),
            error: None,
        }
    }

    pub fn finish(self) -> Result<Owned<CAP>, CapacityError> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.buffer),
        }
    }
}

impl<const CAP: u32> Default for BoundedSink<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, src: &[u8]);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn as_mut_slice(&mut self) -> &mut [u8];
}

impl<const CAP: u32> Sink for BoundedSink<CAP> {
    fn push(&mut self, byte: u8) {
        if self.error.is_none()
            && let Err(error) = self.buffer.try_push(byte)
        {
            self.error = Some(error);
        }
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        if self.error.is_none()
            && let Err(error) = self.buffer.try_extend_from_slice(src)
        {
            self.error = Some(error);
        }
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
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
