use std::num;

use crate::failure;
use crate::failure::wire::{self, turn};
use crate::protocol::topology::records::consumer::Record;
use crate::protocol::wire::text::{resident, short};
use crate::state::channel::consumer::storage::registry::Registry;
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::consumer::storage::status::Status;
use crate::state::channel::consumer::{self, replay};

pub(crate) struct Read<'a, 'b, 'd, const ID: u8> {
    registry: &'a Registry<'b, 'd, ID>,
}

impl<'b, 'a, 'd, const ID: u8> Read<'a, 'b, 'd, ID> {
    pub(in crate::state::channel::consumer) const fn new(set: &'a Registry<'b, 'd, ID>) -> Self {
        Self { registry: set }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    pub(crate) fn has_replay(&self) -> bool {
        (0..self.registry.slots.capacity()).any(|index| {
            self.registry
                .slots
                .key_value_at(index)
                .is_some_and(|entry| {
                    let slot = entry.1;
                    matches!(
                        slot.status,
                        Status::Active | Status::Recovering | Status::Replaying
                    )
                })
        })
    }

    pub(crate) fn next_replay(&self, after: Option<usize>) -> Option<replay::Item<'a, 'b, 'd, ID>> {
        let start = match after {
            Some(cursor) => cursor.checked_add(1)?,
            None => 0,
        };
        (start..self.registry.slots.capacity()).find_map(|index| {
            let (key, slot) = self.registry.slots.key_value_at(index)?;
            (slot.status == Status::Recovering)
                .then(|| replay::Item::new(index, consumer::Key::from_raw(key), &slot.spec))
        })
    }

    pub(crate) fn cancel_key(&self, id: num::NonZero<u64>) -> Option<consumer::Key> {
        self.registry.cancel_key(id)
    }

    pub(crate) fn snapshot(&self, key: consumer::Key) -> Option<(&'a Record<'b, 'd, ID>, u64)> {
        let slot = self.registry.get(key)?;
        Some((&slot.spec, slot.buffered()))
    }

    pub(crate) fn tag(&self, key: consumer::Key) -> Option<short::Short<'a>> {
        self.registry.get(key).map(Slot::tag)
    }

    pub(crate) fn retiree(
        &self,
        key: consumer::Key,
    ) -> Result<resident::Short<'b, 'd>, failure::Error<resident::Short<'b, 'd>>> {
        let slot = match self.registry.slots.key_value_at(key.index() as usize) {
            Some((actual, slot)) if actual == key.raw() => slot,
            entry => {
                return Err(failure::Error::Protocol(wire::Fault::ConsumerKey {
                    expected: Some(num::NonZero::from(key)),
                    actual: entry
                        .map(|(actual, _)| num::NonZero::from(consumer::Key::from_raw(actual))),
                    step: turn::Step::Pending,
                }));
            }
        };
        if slot.status != Status::Retiring {
            return Err(failure::Error::Protocol(wire::Fault::ConsumerPhase {
                consumer: num::NonZero::from(key),
                expected: turn::Consumer::Retiring,
                actual: slot.status.phase(),
            }));
        }
        slot.spec.try_tag().map_err(failure::Error::Retain)
    }
}
