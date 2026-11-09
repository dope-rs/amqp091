use std::{ptr, str};

use crate::protocol::wire::text::short::Short;

use super::Inline;

impl Inline {
    pub(super) fn initialize(&mut self, value: Short<'_>, hash: u64) {
        let bytes = value.value().as_bytes();
        // SAFETY: Short admits at most 255 UTF-8 bytes. This exclusive writer
        // has room for that exact prefix and cannot alias the borrowed source.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), self.bytes.as_mut_ptr().cast(), bytes.len());
        }
        self.len = value.len();
        self.hash = hash;
    }

    pub(super) fn as_str(&self) -> &str {
        let bytes = &self.bytes[..usize::from(self.len)];
        // SAFETY: initialize copies the complete Short before publishing len.
        // The retained prefix is initialized UTF-8; the suffix is never read.
        unsafe { str::from_utf8_unchecked(bytes.assume_init_ref()) }
    }
}
