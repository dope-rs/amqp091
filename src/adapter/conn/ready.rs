use std::mem;

use crate::adapter::conn::Epoch;
use crate::state::epoch;

/// Proof that one transport epoch has completed service-owned topology replay.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ready<'d, const ID: u8> {
    epoch: Epoch<'d, ID>,
}

impl<'d, const ID: u8> Ready<'d, ID> {
    pub(in crate::adapter) const fn from_raw(raw: epoch::Id<'d, ID>) -> Self {
        Self {
            epoch: Epoch::from_raw(raw),
        }
    }

    pub const fn epoch(self) -> Epoch<'d, ID> {
        self.epoch
    }
}

const _: () = assert!(mem::size_of::<Ready<'static, 0>>() == mem::size_of::<Epoch<'static, 0>>());
