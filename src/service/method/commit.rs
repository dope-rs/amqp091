use std::mem;

use crate::state::epoch;

#[repr(transparent)]
#[must_use = "terminal reply commit must govern exact transport retirement"]
pub(in crate::service) struct Commit<'d, const ID: u8>(epoch::Id<'d, ID>);

impl<'d, const ID: u8> Commit<'d, ID> {
    pub(in crate::service) const fn new(epoch: epoch::Id<'d, ID>) -> Self {
        Self(epoch)
    }

    pub(in crate::service) const fn epoch(&self) -> epoch::Id<'d, ID> {
        self.0
    }
}

const _: () =
    assert!(mem::size_of::<Commit<'static, 0>>() == mem::size_of::<epoch::Id<'static, 0>>());
