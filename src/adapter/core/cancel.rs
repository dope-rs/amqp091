use std::marker;

use crate::adapter::core;
use crate::failure;
use crate::service::cancel::pending::{self, reap};
use crate::service::{self, cancel};
use crate::state::channel::cancel::lanes::{self, Slot};
use crate::state::channels;

/// Lane-scoped cancellation authority over the original coordinator owner.
#[repr(transparent)]
pub(in crate::adapter) struct Cancel<'o, 'd: 'o, const ID: u8, K: core::Capability> {
    owner: &'o service::Coord<'d, ID>,
    capability: marker::PhantomData<fn() -> K>,
}

impl<'o, 'd: 'o, const ID: u8, K: core::Capability> Cancel<'o, 'd, ID, K> {
    pub(super) const fn new(owner: &'o service::Coord<'d, ID>) -> Self {
        Self {
            owner,
            capability: marker::PhantomData,
        }
    }

    pub(in crate::adapter) fn try_bind(
        self,
        ticket: &cancel::Ticket<'o, 'd, lanes::Consumer, ID>,
        lease: channels::Lease<'d, ID>,
    ) -> Result<(), failure::Error> {
        ticket.try_bind(self.owner, lease)
    }

    pub(in crate::adapter) fn issue<L: cancel::Lane<'d, ID>>(
        self,
        index: channels::Index<'_, ID>,
        lease: channels::Lease<'d, ID>,
        slot: Slot<L>,
        identity: L::Identity,
    ) -> Result<cancel::Ticket<'o, 'd, L, ID>, failure::Error>
    where
        K: core::Allows<core::Issue<L>>,
    {
        cancel::Ticket::try_new(self.owner, index, lease, slot, identity)
            .map_err(failure::Error::Cancellation)
    }

    pub(in crate::adapter) fn reap_any<L: cancel::Lane<'d, ID>>(
        self,
        reap: impl FnOnce(pending::Pending<'d, L, ID>) -> reap::Decision,
    ) where
        K: core::Allows<core::Reap<L>>,
    {
        self.owner.cancellations.reap_any(reap);
    }

    pub(in crate::adapter) fn try_reap<L: cancel::Lane<'d, ID>, R>(
        self,
        index: channels::Index<'_, ID>,
        lease: channels::Lease<'d, ID>,
        reap: impl FnOnce(pending::Pending<'d, L, ID>) -> Option<R>,
    ) -> Result<Option<R>, failure::Error>
    where
        K: core::Allows<core::Reap<L>>,
    {
        self.owner
            .cancellations
            .try_reap(index, lease, reap)
            .map_err(failure::Error::Cancellation)
    }

    pub(in crate::adapter) fn reap<L: cancel::Lane<'d, ID>>(
        self,
        index: channels::Index<'_, ID>,
        lease: channels::Lease<'d, ID>,
        reap: impl FnOnce(pending::Pending<'d, L, ID>) -> reap::Decision,
    ) -> Result<(), failure::Error>
    where
        K: core::Allows<core::Reap<L>>,
    {
        self.try_reap::<L, _>(index, lease, |pending| {
            matches!(reap(pending), reap::Decision::Remove).then_some(())
        })
        .map(|_| ())
    }
}
