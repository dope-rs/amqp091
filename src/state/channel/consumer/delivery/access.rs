use dope::core::driver::schedule::ready::wake::{self, store};
use o3::depot::arena::hash::runtime;

use crate::protocol::wire::Fingerprint;
use crate::state::channel::consumer::delivery::{Claim, Next, Route, Store};
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::consumer::storage::status::Status;
use crate::state::channel::{self, consumer};

/// Exclusive delivery operations tied to one registry and its exact pool.
pub(in crate::state::channel::consumer) struct Access<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    slots: &'a mut runtime::Registry<Slot<'b, 'd, ID>, consumer::Tag>,
    store: &'r Store<'c, 'b, 'd>,
    wakes: store::Store<'d>,
}

impl<'r, 'c, 'b, 'a, 'd, const ID: u8> Access<'a, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::state::channel::consumer) const fn new(
        slots: &'a mut runtime::Registry<Slot<'b, 'd, ID>, consumer::Tag>,
        store: &'r Store<'c, 'b, 'd>,
        wakes: store::Store<'d>,
    ) -> Self {
        Self {
            slots,
            store,
            wakes,
        }
    }

    pub(in crate::state::channel::consumer) fn clear(self) -> channel::Freed {
        let mut freed = channel::Freed::Available;
        for index in 0..self.slots.capacity() {
            if let Some(mut entry) = self.slots.entry_at(index) {
                freed = freed.merge(self.store.drain(&mut entry.get_mut().queue));
            }
        }
        freed
    }

    pub(in crate::state::channel::consumer) fn next(
        self,
        key: consumer::Key,
        wake: wake::Waker<'d>,
    ) -> Next<'c, 'b, 'd> {
        let Some(entry) = self.slots.entry(key.raw()) else {
            return Next::Closed;
        };
        let slot = entry.into_mut();
        match slot.status {
            Status::Active | Status::Untracked => {
                let (delivery, freed) = self.store.pop(&mut slot.queue);
                if let Some(delivery) = delivery {
                    return Next::Delivery { delivery, freed };
                }
                self.wakes.set(&slot.waker, wake);
                Next::Pending
            }
            Status::Recovering | Status::Pending | Status::Replaying => Next::Unavailable,
            Status::Canceled | Status::Retiring => Next::Closed,
        }
    }

    pub(in crate::state::channel::consumer) fn reserve(
        self,
        tag: &str,
    ) -> Option<Route<'r, 'c, 'b, 'd>> {
        let Some(entry) = self
            .slots
            .find(Fingerprint::of(tag.as_bytes()).get(), |slot| {
                slot.tag().value() == tag
            })
        else {
            return Some(Route::Discard);
        };
        if !matches!(entry.get().status, Status::Active | Status::Untracked) {
            return Some(Route::Discard);
        }
        self.store.vacant().map(Route::Queue)
    }

    pub(in crate::state::channel::consumer) fn route(self, claim: Claim<'r, 'c, 'b, 'd>) {
        let tag = claim.value().consumer_tag();
        let Some(entry) = self
            .slots
            .find(Fingerprint::of(tag.as_bytes()).get(), |slot| {
                slot.tag().value() == tag
            })
        else {
            return;
        };
        if !matches!(entry.get().status, Status::Active | Status::Untracked) {
            return;
        }
        let slot = entry.into_mut();
        claim.push(&mut slot.queue);
        self.wakes.wake_once(&slot.waker);
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Access<'static, 'static, 'static, 'static, 'static, 0>>()
            == 3 * std::mem::size_of::<usize>()
    );
};
