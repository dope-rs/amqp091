use std::{marker, num};

use dope::core::driver::schedule::ready::wake;
use o3::alloc;
use o3::depot::capacity;
use o3::depot::queue::indexed::fifo::staged;
use wake::store;

use crate::failure::{terminal, wire};
use crate::state::channel::cancel::lanes;
use crate::{failure, protocol};

pub(crate) struct Ledger<'c, 'b, 'd> {
    cursor: Cursor,
    slots: staged::Table<Key<'d>, Pending<'d>, Settled<'c, 'b, 'd>>,
}

struct Cursor {
    raw: u64,
    tag: u64,
}

impl Cursor {
    const TAG_MAX: u64 = i64::MAX as u64;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reap {
    Removed,
    Missing,
}

struct Pending<'d> {
    cancelled: bool,
    wake: store::Slot<'d>,
}

struct Settled<'c, 'b, 'd> {
    outcome: Result<protocol::Confirmation, terminal::Outcome<'c, 'b, 'd>>,
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Key<'d> {
    raw: num::NonZero<u64>,
    brand: marker::PhantomData<&'d mut &'d ()>,
}

impl<'d> Key<'d> {
    const fn new(raw: num::NonZero<u64>) -> Self {
        Self {
            raw,
            brand: marker::PhantomData,
        }
    }

    fn index(self, cap: num::NonZero<u32>) -> u32 {
        ((self.raw.get() - 1) % u64::from(cap.get())) as u32
    }

    fn try_index(self, cap: capacity::Count) -> Option<u32> {
        Some(self.index(cap.nonzero()?))
    }
}

impl<'c, 'b, 'd> Ledger<'c, 'b, 'd> {
    pub(crate) fn try_new(cap: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            cursor: Cursor { raw: 0, tag: 0 },
            slots: staged::Table::try_count(cap)?,
        })
    }

    pub(crate) const fn published(&self) -> u64 {
        self.cursor.tag
    }

    pub(crate) fn try_reserve<R>(
        &mut self,
        admit: impl FnOnce(lanes::Slot<lanes::Confirm>, Key<'d>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        let Self { cursor, slots, .. } = self;
        let used = slots.len();
        let max = slots.capacity();
        let Some(entry) = max
            .nonzero()
            .filter(|_| used < max.get())
            .and_then(|cap| slots.entry((cursor.raw % u64::from(cap.get())) as u32))
        else {
            return Err(failure::Error::Capacity(wire::Capacity::PublishConfirm {
                used,
                max,
            }));
        };
        let Some(tag) = num::NonZero::<u64>::MIN
            .checked_add(cursor.tag)
            .filter(|tag| tag.get() <= Cursor::TAG_MAX)
        else {
            return Err(failure::Error::Capacity(wire::Capacity::DeliveryTag {
                current: cursor.tag,
                max: Cursor::TAG_MAX,
            }));
        };
        let Some(raw) = num::NonZero::<u64>::MIN.checked_add(cursor.raw) else {
            return Err(failure::Error::Capacity(
                wire::Capacity::ConfirmGeneration {
                    current: cursor.raw,
                    tag: tag.get(),
                },
            ));
        };
        let key = Key::new(raw);
        let entry = match entry {
            staged::Entry::Vacant(entry) => entry,
            staged::Entry::Queued(entry) => {
                return Err(failure::Error::Capacity(wire::Capacity::ConfirmSlot {
                    next: key.raw.get(),
                    occupied: entry.key().raw.get(),
                    index: entry.index(),
                }));
            }
            staged::Entry::Parked(entry) => {
                return Err(failure::Error::Capacity(wire::Capacity::ConfirmSlot {
                    next: key.raw.get(),
                    occupied: entry.key().raw.get(),
                    index: entry.index(),
                }));
            }
        };
        let admitted = admit(lanes::Slot::new(entry.index()), key)?;
        entry.insert(
            key,
            Pending {
                cancelled: false,
                wake: store::Slot::empty(),
            },
        );
        cursor.raw = raw.get();
        cursor.tag = tag.get();
        Ok(admitted)
    }

    pub(crate) fn poll(
        &mut self,
        key: Key<'d>,
        wake: wake::Waker<'d>,
    ) -> Option<Result<Option<protocol::Confirmation>, terminal::Outcome<'c, 'b, 'd>>> {
        let index = key.try_index(self.slots.capacity())?;
        match self.slots.entry(index) {
            Some(staged::Entry::Parked(entry)) if *entry.key() == key => {
                Some(entry.take().1.outcome.map(Some))
            }
            Some(staged::Entry::Queued(mut entry)) if *entry.key() == key => {
                entry.as_mut().wake.set(wake);
                Some(Ok(None))
            }
            Some(_) | None => None,
        }
    }

    pub(crate) fn complete(
        &mut self,
        published: u64,
        tag: u64,
        many: bool,
        out: protocol::Confirmation,
    ) -> Result<(), failure::Error> {
        if many {
            let tag = if tag == 0 {
                if published == 0 {
                    return Ok(());
                }
                published
            } else {
                tag
            };
            let key = self.try_key(tag, true, published)?;
            while let Some(entry) = self.slots.front() {
                if entry.key().raw > key.raw {
                    break;
                }
                Self::settle(entry, out);
            }
            return Ok(());
        }
        let key = self.try_key(tag, false, published)?;
        if let Some(index) = key.try_index(self.slots.capacity()) {
            match self.slots.entry(index) {
                Some(staged::Entry::Queued(entry)) if *entry.key() == key => {
                    Self::settle(entry, out);
                }
                Some(_) | None => {}
            }
        }
        Ok(())
    }

    pub(crate) fn retire_epoch(&mut self) {
        self.slots.clear();
        self.cursor.tag = 0;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub(crate) fn close(&mut self) {
        while let Some(entry) = self.slots.front() {
            entry.take().1.wake.wake_once();
        }
    }

    pub(crate) fn reap(&mut self, raw: num::NonZero<u64>) -> Reap {
        let key = Key::new(raw);
        let Some(index) = key.try_index(self.slots.capacity()) else {
            return Reap::Missing;
        };
        match self.slots.entry(index) {
            Some(staged::Entry::Queued(mut entry)) if *entry.key() == key => {
                let item = entry.as_mut();
                item.cancelled = true;
                item.wake.clear();
            }
            Some(staged::Entry::Parked(entry)) if *entry.key() == key => entry.clear(),
            Some(_) | None => return Reap::Missing,
        }
        Reap::Removed
    }

    pub(crate) fn fail_all(&mut self, cause: &terminal::Lease<'c, 'b, 'd>) {
        for index in 0..self.slots.capacity().raw() {
            if let Some(staged::Entry::Parked(mut entry)) = self.slots.entry(index)
                && let Err(out) = &mut entry.as_mut().outcome
            {
                out.replace(cause);
            }
        }
        while let Some(entry) = self.slots.front() {
            if entry.as_ref().cancelled {
                entry.clear();
                continue;
            }
            let item = Settled {
                outcome: Err(terminal::Outcome::retain(cause)),
            };
            entry.set_parked(item).wake.wake_once();
        }
    }

    /// The receive cutoff belongs to this live epoch. Commits preserve raw - tag.
    fn try_key(&self, tag: u64, many: bool, published: u64) -> Result<Key<'d>, failure::Error> {
        let Some(tag) = num::NonZero::new(tag).filter(|tag| tag.get() <= published) else {
            return Err(failure::Error::Protocol(wire::Fault::ConfirmTag {
                tag,
                published,
                multiple: many,
            }));
        };
        Ok(Key::new(
            tag.saturating_add(self.cursor.raw - self.cursor.tag),
        ))
    }

    fn settle(
        entry: staged::Slot<'_, Key<'d>, Pending<'d>, Settled<'c, 'b, 'd>, staged::Queued>,
        out: protocol::Confirmation,
    ) {
        if entry.as_ref().cancelled {
            entry.clear();
            return;
        }
        let item = Settled { outcome: Ok(out) };
        entry.set_parked(item).wake.wake_once();
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Key<'_>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Key<'_>>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Pending<'_>>() == 3 * mem::size_of::<u64>());
    assert!(mem::size_of::<Settled<'_, '_, '_>>() == 2 * mem::size_of::<u64>());
    assert!(staged::Table::<Key<'_>, Pending<'_>, Settled<'_, '_, '_>>::SLOT_BYTES == 48);
    assert!(mem::size_of::<Ledger<'_, '_, '_>>() == 6 * mem::size_of::<u64>());
};

impl From<Key<'_>> for num::NonZero<u64> {
    fn from(value: Key<'_>) -> Self {
        value.raw
    }
}
