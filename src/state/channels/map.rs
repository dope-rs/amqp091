use std::num;

use dope::core::driver;
use o3::base::marker::ThreadBound;
use o3::depot;
use o3::depot::index::bitmap;
use o3::depot::index::bitmap::entry;

use crate::failure::{self, wire};
use crate::permits;
use crate::state::channels::store::Store;
use crate::state::channels::table::Slot;
use crate::state::channels::{Error, Index, Lease, Plan};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Generation(num::NonZero<u64>);

#[repr(C)]
pub(super) struct Map<'r, 'c, 'b, 'd, const ID: u8> {
    store: Store<'r, 'c, 'b, 'd, ID>,
    generation: Option<Generation>,
    free: Option<Index<'c, ID>>,
    thread: ThreadBound,
}

#[must_use = "an occupied channel must be retained or vacated"]
pub(super) struct Occupied<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    row: entry::Row<&'a mut Map<'r, 'c, 'b, 'd, ID>>,
    index: Index<'c, ID>,
}

impl Generation {
    const FIRST: Self = Self(num::NonZero::<u64>::MIN.saturating_add(u16::MAX as u64));

    fn after(current: Option<Self>) -> Result<Self, failure::Error> {
        let Some(current) = current else {
            return Ok(Self::FIRST);
        };
        let next = current
            .0
            .checked_add(Self::FIRST.0.get())
            .ok_or_else(|| failure::Error::Capacity(wire::Capacity::ChannelGeneration))?;
        Ok(Self(next))
    }

    pub(super) fn pack(self, id: u16) -> num::NonZero<u64> {
        self.0 | u64::from(id)
    }

    pub(crate) fn admits<const ID: u8>(self, lease: Lease<'_, ID>) -> bool {
        num::NonZero::from(lease).get() & !u64::from(u16::MAX) <= self.0.get()
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Map<'r, 'c, 'b, 'd, ID> {
    pub(super) fn try_index_mut(
        &mut self,
        key: Lease<'d, ID>,
    ) -> Result<(Index<'c, ID>, &mut Slot<'r, 'c, 'b, 'd, ID>), wire::Fault> {
        let id = usize::from(key.id());
        let failure = |actual: Option<Lease<'d, ID>>| wire::Fault::ControlRoute {
            channel: key.id(),
            expected: num::NonZero::from(key),
            actual: actual.map(num::NonZero::from),
        };
        if id >= self.store.shape.bits() {
            return Err(failure(None));
        }
        let index = unsafe { *self.store.memory.map().add(id) }.ok_or_else(|| failure(None))?;
        let entry = unsafe { self.store.memory.entry_mut(index) };
        let actual = entry.key;
        if actual != Some(key) {
            return Err(failure(actual));
        }
        Ok((index, &mut entry.value))
    }

    pub(super) fn try_new(
        plan: Plan,
        driver: driver::Reference<'d>,
        permits: &permits::Registry<'c, 'd>,
    ) -> Result<Self, Error> {
        let store = Store::try_new(plan, driver, permits)?;
        let free = store.last();
        let table = Self {
            store,
            generation: None,
            free,
            thread: ThreadBound::NEW,
        };
        Ok(table)
    }

    pub(super) fn acquire<R, const EXACT: bool>(
        &mut self,
        bound: u16,
        init: impl FnOnce(Index<'c, ID>, Lease<'d, ID>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        let capacity = if EXACT {
            self.store.slots()
        } else {
            self.store.capacity(bound)
        };
        let vacancy = if EXACT {
            self.store.vacant(bound)?
        } else {
            let Some(vacancy) = self.store.first_vacant() else {
                return Err(failure::Error::ChannelCapacity { capacity });
            };
            if vacancy.id() > bound {
                return Err(failure::Error::ChannelCapacity { capacity });
            }
            vacancy
        };
        let next = Generation::after(self.generation)?;
        let index = self
            .free
            .ok_or_else(|| failure::Error::ChannelCapacity { capacity })?;
        let key = Lease::mint(vacancy.id(), next);
        let (free, result) = vacancy.occupy(index, key, || init(index, key))?;
        self.free = free;
        self.generation = Some(next);
        Ok(result)
    }

    pub(super) fn key_at(&self, id: u16) -> Option<Lease<'d, ID>> {
        self.store.key_at(id)
    }

    pub(super) fn generation(&self) -> Option<Generation> {
        self.generation
    }

    pub(super) fn occupied(
        &mut self,
        key: Lease<'d, ID>,
    ) -> Option<Occupied<'_, 'r, 'c, 'b, 'd, ID>> {
        let row = entry::Row::try_new(self, usize::from(key.id())).ok()?;
        let memory = &row.source().store.memory;
        let index = (unsafe { *memory.map().add(row.index()) })?;
        if unsafe { memory.entry(index).key } != Some(key) {
            return None;
        }
        Some(Occupied { row, index })
    }

    pub(super) fn next(
        &self,
        after: Option<Index<'c, ID>>,
    ) -> Option<(Index<'c, ID>, Lease<'d, ID>, &Slot<'r, 'c, 'b, 'd, ID>)> {
        let upper = self.store.last()?;
        let mut index = upper.after(after)?;
        loop {
            if let Some((key, value)) = self.store.occupied(index) {
                return Some((index, key, value));
            }
            index = upper.after(Some(index))?;
        }
    }

    pub(super) fn visit(&mut self, mut visit: impl FnMut(&mut Slot<'r, 'c, 'b, 'd, ID>)) {
        let Some(upper) = self.store.last() else {
            return;
        };
        let mut index = upper.after(None);
        while let Some(current) = index {
            if let Some((_, slot)) = self.store.occupied_mut(current) {
                visit(slot);
            }
            index = upper.after(Some(current));
        }
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Occupied<'_, 'r, 'c, 'b, 'd, ID> {
    pub(super) fn get_mut(&mut self) -> &mut Slot<'r, 'c, 'b, 'd, ID> {
        // SAFETY: the slot value cannot change the retained table's bitmap storage or domain.
        unsafe { self.row.source_mut() }.store.value_mut(self.index)
    }

    pub(super) fn vacate(mut self) {
        let id = self.row.index();
        // SAFETY: this exclusive entry changes only its own mapping and free-list membership.
        let table = unsafe { self.row.source_mut() };
        unsafe {
            table.store.memory.map_mut().add(id).write(None);
            let entry = table.store.memory.entry_mut(self.index);
            entry.key = None;
            entry.next = table.free;
        }
        table.free = Some(self.index);
        self.row.update::<true>(());
    }
}

// SAFETY: Map retains and exclusively owns the same Store and its immutable bitmap domain.
unsafe impl<const ID: u8> depot::raw::Words for Map<'_, '_, '_, '_, ID> {
    type Plane = ();
    type Size = u32;

    fn shape(&self) -> bitmap::Shape {
        self.store.shape()
    }

    fn words(&self, (): ()) -> &[bitmap::Word] {
        self.store.words(())
    }

    fn row(entry: entry::Row<&Self>, (): ()) -> &[bitmap::Word] {
        // SAFETY: project the exact retained Map into its original Store without changing its domain.
        Store::row(unsafe { entry.map(|table| &table.store) }, ())
    }
}

const _: () = assert!(
    std::mem::size_of::<Occupied<'static, 'static, 'static, 'static, 'static, 0>>()
        == 3 * std::mem::size_of::<usize>()
);
