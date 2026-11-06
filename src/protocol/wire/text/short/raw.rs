use std::{mem, ptr, slice, str};

use crate::protocol::wire::text::short::{InlineName, Name, Short};

/// # Safety
/// `value` must retain an admitted Name's bound of at most 127 bytes.
pub(in crate::protocol) const unsafe fn name(value: Short<'_>) -> Name<'_> {
    Name(value)
}

impl InlineName {
    pub(crate) const EMPTY: Self = Self {
        bytes: [mem::MaybeUninit::uninit(); Name::MAX as usize],
        len: 0,
    };

    /// # Safety
    /// `target` must be writable InlineName storage disjoint from `value`.
    pub(in crate::protocol) unsafe fn initialize(target: *mut Self, value: Name<'_>) {
        // SAFETY: Name admits this initialized UTF-8 prefix of at most 127
        // bytes; the caller owns its disjoint destination storage.
        unsafe {
            ptr::copy_nonoverlapping(
                value.value().as_ptr(),
                ptr::addr_of_mut!((*target).bytes).cast(),
                usize::from(value.len()),
            );
            ptr::addr_of_mut!((*target).len).write(value.len());
        }
    }

    pub(crate) fn name(&self) -> Name<'_> {
        // SAFETY: EMPTY exposes no bytes; initialize copies an admitted Name before
        // publishing len, so this borrow exposes only its initialized prefix.
        let bytes =
            unsafe { slice::from_raw_parts(self.bytes.as_ptr().cast(), usize::from(self.len)) };
        Name(Short(unsafe { str::from_utf8_unchecked(bytes) }))
    }
}

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
