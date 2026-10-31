use std::{marker, num};

use crate::service::cancel::Lane;
use crate::state::channels;

pub(crate) mod reap;

pub(crate) struct Pending<'d, L: Lane<'d, ID>, const ID: u8> {
    pub(super) lease: channels::Lease<'d, ID>,
    pub(super) identity: num::NonZero<u64>,
    lane: marker::PhantomData<fn(L) -> L>,
}

impl<'d, L: Lane<'d, ID>, const ID: u8> Pending<'d, L, ID> {
    pub(super) const fn new(lease: channels::Lease<'d, ID>, identity: num::NonZero<u64>) -> Self {
        Self {
            lease,
            identity,
            lane: marker::PhantomData,
        }
    }

    pub(crate) const fn lease(self) -> channels::Lease<'d, ID> {
        self.lease
    }

    pub(crate) const fn identity(self) -> num::NonZero<u64> {
        self.identity
    }
}

impl<'d, L: Lane<'d, ID>, const ID: u8> Copy for Pending<'d, L, ID> {}

impl<'d, L: Lane<'d, ID>, const ID: u8> Clone for Pending<'d, L, ID> {
    fn clone(&self) -> Self {
        *self
    }
}
