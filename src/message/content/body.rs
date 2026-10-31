use std::io;

use o3::alloc::{self, VecExt as _};
use o3::buffer::segments::scatter;

use crate::service::content::{payload, pending};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Body<'a, 'b, 'd>(&'a payload::Payload<'b, 'd>);

impl<'a, 'b, 'd> Body<'a, 'b, 'd> {
    pub(crate) fn new<K>(lease: &'a pending::Lease<'_, 'b, 'd, K>) -> Self {
        Self(lease.payload())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> Option<&'a [u8]> {
        self.0.as_slice()
    }

    pub fn chunks(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.0.chunks()
    }

    /// Reads retained body segments without materializing a contiguous copy.
    pub fn reader(self) -> impl io::BufRead + 'a
    where
        'd: 'a,
    {
        scatter::Reader::new(self.0.chunks())
    }

    pub fn try_to_vec(&self) -> Result<Vec<u8>, alloc::Error> {
        let mut body = Vec::try_new(self.len())?;
        for chunk in self.chunks() {
            body.extend_from_slice(chunk);
        }
        Ok(body)
    }
}

impl PartialEq<[u8]> for Body<'_, '_, '_> {
    fn eq(&self, other: &[u8]) -> bool {
        self.len() == other.len()
            && self
                .chunks()
                .flat_map(|chunk| chunk.iter().copied())
                .eq(other.iter().copied())
    }
}

const _: () = assert!(size_of::<Body<'static, 'static, 'static>>() == size_of::<usize>());
const _: () = assert!(
    size_of::<scatter::Reader<'static, payload::Chunks<'static, 'static>, [u8]>>()
        == size_of::<payload::Chunks<'static, 'static>>() + size_of::<&'static [u8]>()
);
