use o3::depot::queue::round::{self, values};
use o3::mem::Return;

use crate::failure::{self, wire::Fault};
use crate::service::Coord;
use crate::service::cancel::{Lane, registry};
use crate::state::channel::cancel::lanes;
use crate::state::channels;

#[must_use = "an exact cancellation ticket must be retained or used"]
pub struct Ticket<'a, 'd, L: Lane<'d, ID>, const ID: u8> {
    entry: values::Snapshot<'a, registry::Active<'d, ID>, { lanes::Kind::COUNT }, round::Place>,
    identity: L::Identity,
    owner: &'a Coord<'d, ID>,
}

impl<'d, L: Lane<'d, ID>, const ID: u8> Return for Ticket<'_, 'd, L, ID> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.cancel();
    }
}

impl<'a, 'd, L: Lane<'d, ID>, const ID: u8> Ticket<'a, 'd, L, ID> {
    pub(crate) fn try_new(
        owner: &'a Coord<'d, ID>,
        index: channels::Index<'_, ID>,
        lease: channels::Lease<'d, ID>,
        slot: lanes::Slot<L>,
        identity: L::Identity,
    ) -> Result<Self, round::Bounds> {
        let entry = owner.cancellations.activate(index, slot, lease, identity)?;
        Ok(Self {
            entry,
            identity,
            owner,
        })
    }

    pub(crate) const fn identity(&self) -> L::Identity {
        self.identity
    }

    pub(crate) fn try_bind(
        &self,
        owner: &Coord<'d, ID>,
        lease: channels::Lease<'d, ID>,
    ) -> Result<(), failure::Error> {
        if !std::ptr::eq(self.owner, owner) {
            return Err(failure::Error::Protocol(Fault::ConsumerOwner));
        }
        let identity = self.identity.into();
        let active = match self.entry.slot().get() {
            Some(active) if active.identity == identity => active,
            active => {
                return Err(failure::Error::Protocol(Fault::ConsumerExpired {
                    expected: identity,
                    actual: active.map(|active| active.identity),
                }));
            }
        };
        let expected = active.lease();
        if expected != lease {
            return Err(failure::Error::Protocol(Fault::ConsumerLease {
                expected: expected.into(),
                actual: lease.into(),
            }));
        }
        Ok(())
    }

    pub(crate) fn complete(self) {
        let slot = self.entry.slot();
        if slot.get().map(|active| active.identity) == Some(self.identity.into()) {
            let _ = slot.take();
        }
    }

    pub(crate) fn cancel(&self) {
        // Queue membership keeps Coord runnable until this cancellation is drained.
        let _ = self
            .entry
            .set(|active| (active.identity == self.identity.into()).then_some(()));
    }
}
