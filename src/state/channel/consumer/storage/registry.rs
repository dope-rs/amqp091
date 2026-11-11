use std::num;

use dope::core::driver::schedule::ready::wake::{self, store};
use o3::alloc;
use o3::depot::arena::hash::runtime::{self, entry};

use crate::failure::wire;
use crate::failure::wire::turn;
use crate::failure::wire::turn::Consumer;
use crate::protocol::wire::Fingerprint;
use crate::state::channel::consumer::delivery::{Claim, Next, Route, Store};
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::{self, consumer};

pub(in crate::state::channel::consumer) struct Registry<'b, 'd, const ID: u8> {
    pub(in crate::state::channel::consumer) slots:
        runtime::Registry<Slot<'b, 'd, ID>, consumer::Tag>,
}

impl<'b, 'd, const ID: u8> Registry<'b, 'd, ID> {
    pub(in crate::state::channel::consumer) fn try_new(
        plan: consumer::Plan,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: runtime::Registry::<Slot<'b, 'd, ID>, consumer::Tag>::try_new(plan.resident)?,
        })
    }

    pub(in crate::state::channel::consumer) fn clear<'c>(
        &mut self,
        values: &Store<'c, 'b, 'd>,
    ) -> channel::Freed {
        let mut freed = channel::Freed::Available;
        for index in 0..self.slots.capacity() {
            if let Some(mut entry) = self.slots.entry_at(index) {
                freed = freed.merge(values.drain(&mut entry.get_mut().queue));
            }
        }
        freed
    }

    pub(in crate::state::channel::consumer) fn next<'c>(
        &mut self,
        values: &Store<'c, 'b, 'd>,
        wakes: store::Store<'d>,
        key: consumer::Key,
        wake: wake::Waker<'d>,
    ) -> Next<'c, 'b, 'd> {
        let Some(entry) = self.slots.entry(key.raw()) else {
            return Next::Closed;
        };
        let slot = entry.into_mut();
        match slot.status {
            Consumer::Active | Consumer::Untracked => {
                let (delivery, freed) = values.pop(&mut slot.queue);
                if let Some(delivery) = delivery {
                    return Next::Delivery { delivery, freed };
                }
                wakes.set(&slot.waker, wake);
                Next::Pending
            }
            Consumer::Recovering | Consumer::Pending | Consumer::Replaying => Next::Unavailable,
            Consumer::Canceled | Consumer::Retiring => Next::Closed,
        }
    }

    pub(in crate::state::channel::consumer) fn reserve<'r, 'c>(
        &mut self,
        values: &'r Store<'c, 'b, 'd>,
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
        if !matches!(entry.get().status, Consumer::Active | Consumer::Untracked) {
            return Some(Route::Discard);
        }
        values.vacant().map(Route::Queue)
    }

    pub(in crate::state::channel::consumer) fn route<'r, 'c>(
        &mut self,
        claim: Claim<'r, 'c, 'b, 'd>,
        wakes: store::Store<'d>,
    ) {
        let tag = claim.value().consumer_tag();
        let Some(entry) = self
            .slots
            .find(Fingerprint::of(tag.as_bytes()).get(), |slot| {
                slot.tag().value() == tag
            })
        else {
            return;
        };
        if !matches!(entry.get().status, Consumer::Active | Consumer::Untracked) {
            return;
        }
        let slot = entry.into_mut();
        claim.push(&mut slot.queue);
        wakes.wake_once(&slot.waker);
    }

    pub(in crate::state::channel::consumer) fn key(&self, tag: &str) -> Option<consumer::Key> {
        self.slots
            .key(Fingerprint::of(tag.as_bytes()).get(), |slot| {
                slot.tag().value() == tag
            })
            .map(consumer::Key::from_raw)
    }

    pub(in crate::state::channel::consumer) fn get(
        &self,
        key: consumer::Key,
    ) -> Option<&Slot<'b, 'd, ID>> {
        self.slots.get(key.raw())
    }

    pub(in crate::state::channel::consumer) fn try_entry(
        &mut self,
        key: consumer::Key,
        step: turn::Step,
    ) -> Result<entry::Entry<'_, Slot<'b, 'd, ID>, consumer::Tag>, wire::Fault> {
        match self.slots.entry_at(key.index() as usize) {
            Some(entry) if entry.key() == key.raw() => Ok(entry),
            entry => Err(wire::Fault::ConsumerKey {
                expected: Some(num::NonZero::from(key)),
                actual: entry.map(|entry| num::NonZero::from(consumer::Key::from_raw(entry.key()))),
                step,
            }),
        }
    }

    pub(in crate::state::channel::consumer) fn begin_retire<'r, 'c>(
        &mut self,
        key: consumer::Key,
        store: &'r Store<'c, 'b, 'd>,
        replay: impl FnOnce() -> bool,
    ) -> Option<(&mut Slot<'b, 'd, ID>, channel::Freed)> {
        let (slot, freed) = match self.slots.entry(key.raw())? {
            entry::Entry::Indexed(entry) => {
                if !matches!(
                    entry.get().status,
                    Consumer::Active | Consumer::Untracked | Consumer::Replaying
                ) {
                    return None;
                }
                let slot = entry.unindex().into_mut();
                let freed = store.drain(&mut slot.queue);
                (slot, freed)
            }
            entry::Entry::Staged(entry) => {
                if entry.get().status != Consumer::Canceled || !replay() {
                    return None;
                }
                (entry.into_mut(), channel::Freed::Available)
            }
        };
        slot.status = Consumer::Retiring;
        Some((slot, freed))
    }

    pub(in crate::state::channel::consumer) fn cancel<'r, 'c>(
        &mut self,
        tag: &str,
        store: &'r Store<'c, 'b, 'd>,
    ) -> Option<(&mut Slot<'b, 'd, ID>, channel::Freed)> {
        let entry = self
            .slots
            .find(Fingerprint::of(tag.as_bytes()).get(), |slot| {
                slot.tag().value() == tag
            })?;
        if !matches!(
            entry.get().status,
            Consumer::Active | Consumer::Untracked | Consumer::Recovering | Consumer::Replaying
        ) {
            return None;
        }
        let slot = entry.unindex().into_mut();
        slot.status = Consumer::Canceled;
        let freed = store.drain(&mut slot.queue);
        Some((slot, freed))
    }

    pub(in crate::state::channel::consumer) fn cancel_key(
        &self,
        id: num::NonZero<u64>,
    ) -> Option<consumer::Key> {
        let (key, _) = self.slots.key_value_at(id.get() as u32 as usize)?;
        let key = consumer::Key::from_raw(key);
        (num::NonZero::from(key) == id).then_some(key)
    }
}
