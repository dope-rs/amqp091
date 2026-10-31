use std::str;

use crate::protocol::wire::text::short::Short;

#[must_use = "validated short bytes must be rebound or released"]
#[repr(transparent)]
pub(in crate::protocol::wire) struct Encoded<'a>(&'a [u8]);

impl<'a> Encoded<'a> {
    /// # Safety
    /// `bytes` must be the exact value bytes emitted from a live `Short`.
    pub(in crate::protocol::wire) unsafe fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(in crate::protocol::wire) fn bind(self) -> Short<'a> {
        // SAFETY: construction proves these exact bytes were emitted from valid UTF-8.
        Short(unsafe { str::from_utf8_unchecked(self.0) })
    }
}

const _: () = assert!(std::mem::size_of::<Encoded<'static>>() == 2 * std::mem::size_of::<usize>());
