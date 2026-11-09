use std::io;

use o3::buffer::segments::scatter;

use crate::service::content::{payload, pending};

mod raw;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Body<'a, 'b, 'd>(&'a payload::Payload<'b, 'd>);

impl<'a, 'b, 'd> Body<'a, 'b, 'd> {
    pub(crate) fn new<K>(lease: &'a pending::Lease<'_, 'b, 'd, K>) -> Self {
        // Only completed, frozen content reaches this view: its immutable
        // chunks cover exactly the admitted body length.
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
}

const _: () = assert!(size_of::<Body<'static, 'static, 'static>>() == size_of::<usize>());
const _: () = assert!(
    size_of::<scatter::Reader<'static, payload::Chunks<'static, 'static>>>()
        == size_of::<payload::Chunks<'static, 'static>>() + size_of::<&'static [u8]>()
);
