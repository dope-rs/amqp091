use std::num;

use crate::failure;
use crate::failure::wire::turn::Consumer;
use crate::failure::wire::{self, turn};
use crate::protocol::topology::records::consumer::Record;
use crate::protocol::wire::text::{resident, short};
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::consumer::{self, Set, replay};

impl<'b, 'd, const ID: u8> Set<'b, 'd, ID> {
    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub(crate) fn has_replay(&self) -> bool {
        self.slots.values().any(|slot| {
            matches!(
                slot.status,
                Consumer::Active | Consumer::Recovering | Consumer::Replaying
            )
        })
    }

    pub(crate) fn next_replay(&self, after: Option<usize>) -> Option<replay::Item<'_, 'b, 'd, ID>> {
        let start = match after {
            Some(cursor) => cursor.checked_add(1)?,
            None => 0,
        };
        (start..self.slots.capacity()).find_map(|index| {
            let (key, slot) = self.slots.key_value_at(index)?;
            (slot.status == Consumer::Recovering)
                .then(|| replay::Item::new(index, consumer::Key::from_raw(key), &slot.spec))
        })
    }

    pub(crate) fn snapshot(&self, key: consumer::Key) -> Option<(&Record<'b, 'd, ID>, u64)> {
        let slot = self.get(key)?;
        Some((&slot.spec, slot.buffered()))
    }

    pub(crate) fn tag(&self, key: consumer::Key) -> Option<short::Short<'_>> {
        self.get(key).map(Slot::tag)
    }

    pub(crate) fn retiree(
        &self,
        key: consumer::Key,
    ) -> Result<resident::Short<'b, 'd>, failure::Error<resident::Short<'b, 'd>>> {
        let slot = match self.slots.key_value_at(key.index() as usize) {
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
        if slot.status != Consumer::Retiring {
            return Err(failure::Error::Protocol(wire::Fault::ConsumerPhase {
                consumer: num::NonZero::from(key),
                expected: turn::Consumer::Retiring,
                actual: slot.status,
            }));
        }
        slot.spec.try_tag().map_err(failure::Error::Retain)
    }
}
