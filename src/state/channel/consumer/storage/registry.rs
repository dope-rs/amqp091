use std::num;

use dope::core::driver::schedule::ready::wake::store;
use o3::alloc;
use o3::depot::arena::hash::runtime::{self, entry};

use crate::failure::wire::turn;
use crate::failure::{self, wire};
use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::text::short;
use crate::state::channel::consumer::delivery::{Access, Store};
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::consumer::storage::status::Status;
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

    pub(in crate::state::channel::consumer) fn deliveries<'r, 'c>(
        &mut self,
        values: &'r Store<'c, 'b, 'd>,
        wakes: store::Store<'d>,
    ) -> Access<'_, 'r, 'c, 'b, 'd, ID> {
        Access::new(&mut self.slots, values, wakes)
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

    pub(in crate::state::channel::consumer) fn check(
        &self,
        tag: short::Short<'_>,
    ) -> Result<(), failure::Error> {
        if !tag.is_empty()
            && let Some(actual) = self.key(tag.value())
        {
            return Err(failure::Error::Protocol(wire::Fault::ConsumerKey {
                expected: None,
                actual: Some(num::NonZero::from(actual)),
                step: turn::Step::Pending,
            }));
        }
        if self.slots.is_full() {
            return Err(failure::Error::Capacity(wire::Capacity::Consumer));
        }
        Ok(())
    }

    pub(in crate::state::channel::consumer) fn remove<'r, 'c>(
        &mut self,
        key: consumer::Key,
        store: &'r Store<'c, 'b, 'd>,
    ) -> Option<(Slot<'b, 'd, ID>, channel::Freed)> {
        let mut slot = self.slots.entry(key.raw())?.remove();
        let freed = store.drain(&mut slot.queue);
        Some((slot, freed))
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
                    Status::Active | Status::Untracked | Status::Replaying
                ) {
                    return None;
                }
                let slot = entry.unindex().into_mut();
                let freed = store.drain(&mut slot.queue);
                (slot, freed)
            }
            entry::Entry::Staged(entry) => {
                if entry.get().status != Status::Canceled || !replay() {
                    return None;
                }
                (entry.into_mut(), channel::Freed::Available)
            }
        };
        slot.status = Status::Retiring;
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
            Status::Active | Status::Untracked | Status::Recovering | Status::Replaying
        ) {
            return None;
        }
        let slot = entry.unindex().into_mut();
        slot.status = Status::Canceled;
        let freed = store.drain(&mut slot.queue);
        Some((slot, freed))
    }

    pub(in crate::state::channel::consumer) fn is_empty(&self) -> bool {
        self.slots.is_empty()
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
