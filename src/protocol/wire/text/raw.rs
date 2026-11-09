use std::{mem, str};

use o3::buffer::resident;
use o3::mem::rc;

use crate::protocol::wire::schema;
use crate::protocol::wire::text::fields;

impl<Schema> fields::Packed<Schema> {
    pub(crate) fn get<'a, const INDEX: usize>(
        &self,
        bytes: &'a [mem::MaybeUninit<u8>; fields::PACKED_CAPACITY],
        field: schema::Field<Schema, INDEX>,
    ) -> &'a str {
        const { assert!(INDEX < 3) };
        let start = if INDEX == 0 { 0 } else { self.ends[INDEX - 1] } as usize;
        let end = usize::from(self.ends[field.index()]);
        // SAFETY: copy_from initializes all three admitted short strings before
        // publishing their ends, each at most 765. Frozen retains this matching
        // backing; only the selected initialized range is exposed.
        let bytes = unsafe { bytes.get_unchecked(start..end).assume_init_ref() };
        // SAFETY: each copied range retains its source's validated UTF-8.
        unsafe { str::from_utf8_unchecked(bytes) }
    }
}

impl fields::Span {
    pub(super) fn view(self, backing: &[u8]) -> &str {
        // SAFETY: every span is validated against the immutable backing before publication.
        let bytes = unsafe { backing.get_unchecked(self.start()..self.end()) };
        // SAFETY: the same validation proves the selected bytes are UTF-8.
        unsafe { str::from_utf8_unchecked(bytes) }
    }
}

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
