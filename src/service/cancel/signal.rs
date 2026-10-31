use std::num;

use crate::state::channel::cancel::lanes;
use crate::state::channels;

#[derive(Clone, Copy)]
pub(crate) struct Signal<'d, const ID: u8> {
    pub(super) lease: channels::Lease<'d, ID>,
    pub(super) kind: lanes::Kind,
    pub(super) identity: num::NonZero<u64>,
}

impl<'d, const ID: u8> Signal<'d, ID> {
    pub(crate) const fn lease(self) -> channels::Lease<'d, ID> {
        self.lease
    }

    pub(crate) const fn local(self) -> (lanes::Kind, num::NonZero<u64>) {
        (self.kind, self.identity)
    }
}
