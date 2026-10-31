use std::mem;

use crate::state::epoch;

/// Invariant driver- and route-branded transport generation.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch<'d, const ID: u8> {
    raw: epoch::Id<'d, ID>,
}

impl<'d, const ID: u8> Epoch<'d, ID> {
    pub(in crate::adapter) const fn from_raw(raw: epoch::Id<'d, ID>) -> Self {
        Self { raw }
    }

    pub(in crate::adapter) const fn raw(self) -> epoch::Id<'d, ID> {
        self.raw
    }
}

const _: () =
    assert!(mem::size_of::<Epoch<'static, 0>>() == mem::size_of::<epoch::Id<'static, 0>>());
const _: () =
    assert!(mem::align_of::<Epoch<'static, 0>>() == mem::align_of::<epoch::Id<'static, 0>>());
