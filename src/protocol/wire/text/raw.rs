use std::{mem, str};

use o3::buffer::resident;
use o3::mem::rc;

#[must_use = "validated capsule ownership must be retained or released"]
#[derive(Debug)]
#[repr(C)]
pub(crate) struct Capsule<'d> {
    bytes: resident::Exact<'d>,
    span: Span,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct Span {
    start: u32,
    end: u32,
}

impl<'d> Capsule<'d> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            bytes: self.bytes.try_clone()?,
            span: self.span,
        })
    }

    /// # Safety
    /// `start..start + len` must identify validated UTF-8 in this exact owner.
    pub(crate) unsafe fn from_raw(bytes: resident::Exact<'d>, start: u32, len: u8) -> Self {
        let span = Span {
            start,
            end: start + u32::from(len),
        };
        Self { bytes, span }
    }

    pub(crate) fn as_str(&self) -> &str {
        let start = self.span.start as usize;
        let end = self.span.end as usize;
        // SAFETY: construction binds this validated range to its exact immutable owner.
        let bytes = unsafe { self.bytes.as_slice().get_unchecked(start..end) };
        // SAFETY: construction is restricted to a previously validated short string.
        unsafe { str::from_utf8_unchecked(bytes) }
    }
}

const _: () = {
    assert!(mem::size_of::<Span>() == 2 * mem::size_of::<u32>());
    assert!(
        mem::size_of::<Capsule<'static>>()
            == mem::size_of::<resident::Exact<'static>>() + mem::size_of::<Span>()
    );
};
