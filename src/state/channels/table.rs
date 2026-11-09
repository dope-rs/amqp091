use std::{marker, num};

use dope::core::driver;
use o3::alloc;
use o3::base::marker::ThreadBound;
use o3::depot;
use o3::depot::arena::slab;
use o3::depot::index::bitmap::{self, entry};
use o3::depot::queue::resident;

use crate::failure::{self, wire};
use crate::permits;
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{self};
use crate::state::channel::{self, change, consumer};
use crate::state::channels::Index;
use crate::state::channels::store::Store;
use crate::state::limits;

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    pub(super) id_capacity: usize,
    pub(super) slot_capacity: u32,
    pub(super) limits: limits::Limits,
    pub(super) consumers: consumer::Plan,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Error {
    Allocation(alloc::Error),
    Lane(resident::Bounds<num::NonZero<u32>>),
    Index(num::TryFromIntError),
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl Plan {
    pub(crate) const fn new(
        ids: usize,
        slots: u32,
        limits: limits::Limits,
        spec: consumer::Plan,
    ) -> Self {
        Self {
            id_capacity: ids,
            slot_capacity: slots,
            limits,
            consumers: spec,
        }
    }

    pub(crate) const fn topology(self) -> topology::Plan {
        self.limits.resources.topology
    }

    pub(crate) const fn slots(self) -> usize {
        self.slot_capacity as usize
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lease<'d, const ID: u8> {
    packed: num::NonZero<u64>,
    brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, const ID: u8> Lease<'d, ID> {
    pub(crate) const fn id(self) -> u16 {
        let bytes = self.packed.get().to_le_bytes();
        u16::from_le_bytes([bytes[0], bytes[1]])
    }

    pub(super) fn mint(id: u16, epoch: Generation) -> Self {
        Self {
            packed: epoch.pack(id),
            brand: marker::PhantomData,
        }
    }
}

pub(super) struct Slot<'r, 'c, 'b, 'd, const ID: u8> {
    pub(super) channel: channel::Session<'r, 'c, 'b, 'd, ID>,
    pub(super) local: topology::Local<'d, ID>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Generation(num::NonZero<u64>);

#[repr(C)]
pub(crate) struct Table<'r, 'c, 'b, 'd, const ID: u8> {
    store: Store<'r, 'c, 'b, 'd, ID>,
    generation: Option<Generation>,
    free: Option<Index<'c, ID>>,
    thread: ThreadBound,
}

#[must_use = "an occupied channel must be retained or vacated"]
struct Occupied<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    row: entry::Row<&'a mut Table<'r, 'c, 'b, 'd, ID>>,
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

    fn pack(self, id: u16) -> num::NonZero<u64> {
        self.0 | u64::from(id)
    }

    pub(crate) fn admits<const ID: u8>(self, lease: Lease<'_, ID>) -> bool {
        num::NonZero::from(lease).get() & !u64::from(u16::MAX) <= self.0.get()
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Table<'r, 'c, 'b, 'd, ID> {
    fn try_index_mut(
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

    pub(crate) fn try_new(
        plan: Plan,
        driver: driver::Reference<'d>,
        permits: &permits::Registry<'c, 'd>,
    ) -> Result<Self, Error> {
        let store = Store::try_new(plan, driver, permits)?;
        let free = store.last();
        Ok(Self {
            store,
            generation: None,
            free,
            thread: ThreadBound::NEW,
        })
    }

    pub(crate) fn acquire<R, const EXACT: bool>(
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

    pub(crate) fn lease(&self, id: u16) -> Option<Lease<'d, ID>> {
        self.store.key_at(id)
    }

    pub(crate) fn generation(&self) -> Option<Generation> {
        self.generation
    }

    fn occupied(&mut self, key: Lease<'d, ID>) -> Option<Occupied<'_, 'r, 'c, 'b, 'd, ID>> {
        let row = entry::Row::try_new(self, usize::from(key.id())).ok()?;
        let memory = &row.source().store.memory;
        let index = (unsafe { *memory.map().add(row.index()) })?;
        if unsafe { memory.entry(index).key } != Some(key) {
            return None;
        }
        Some(Occupied { row, index })
    }

    fn next_slot(
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

    pub(crate) fn collect(&mut self, lease: Lease<'d, ID>) {
        let Some(mut entry) = self.occupied(lease) else {
            return;
        };
        let slot = entry.get_mut();
        if slot.local.is_tracked() || !slot.channel.can_release() {
            return;
        }
        slot.channel.release();
        slot.local.clear();
        entry.vacate();
    }

    pub(crate) fn abandon(
        &mut self,
        graph: &mut topology::Table<'c, 'd, ID>,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        lease: Lease<'d, ID>,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> channel::Freed {
        let Some(mut entry) = self.occupied(lease) else {
            return channel::Freed::Available;
        };
        let slot = entry.get_mut();
        let mut state = topology::State::new(&mut slot.local, graph, values);
        let freed = slot
            .channel
            .change(&mut state, stores, change::Event::ChannelClosed);
        state.clear();
        freed
    }

    pub(crate) fn releasable(
        &self,
        after: Option<Index<'c, ID>>,
    ) -> Option<(Index<'c, ID>, Lease<'d, ID>)> {
        self.next(after, |slot| slot.channel.can_release())
    }

    pub(crate) fn next_tracked(
        &self,
        after: Option<Index<'c, ID>>,
    ) -> Option<(Index<'c, ID>, Lease<'d, ID>)> {
        self.next(after, |slot| slot.local.is_tracked())
    }

    pub(crate) fn tracked_len(&self) -> usize {
        let mut count = 0;
        let mut after = None;
        while let Some((index, _)) = self.next_tracked(after) {
            count += 1;
            after = Some(index);
        }
        count
    }

    fn next(
        &self,
        mut after: Option<Index<'c, ID>>,
        select: impl Fn(&Slot<'r, 'c, 'b, 'd, ID>) -> bool,
    ) -> Option<(Index<'c, ID>, Lease<'d, ID>)> {
        loop {
            let (index, lease, slot) = self.next_slot(after)?;
            if select(slot) {
                return Some((index, lease));
            }
            after = Some(index);
        }
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Table<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn try_get_mut(
        &mut self,
        lease: Lease<'d, ID>,
    ) -> Result<
        (
            &mut channel::Session<'r, 'c, 'b, 'd, ID>,
            &mut topology::Local<'d, ID>,
        ),
        wire::Fault,
    > {
        let (_, slot) = self.try_index_mut(lease)?;
        Ok((&mut slot.channel, &mut slot.local))
    }

    pub(crate) fn edit<R>(
        &mut self,
        graph: &mut topology::Table<'c, 'd, ID>,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        lease: Lease<'d, ID>,
        f: impl for<'a, 's, 'v> FnOnce(
            Index<'c, ID>,
            &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
            &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
        ) -> R,
    ) -> Option<R> {
        let (index, slot) = self.try_index_mut(lease).ok()?;
        let mut state = topology::State::new(&mut slot.local, graph, values);
        Some(f(index, &mut slot.channel, &mut state))
    }

    pub(crate) fn visit(
        &mut self,
        graph: &mut topology::Table<'c, 'd, ID>,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        mut f: impl for<'a, 's, 'v> FnMut(
            &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
            &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
        ),
    ) {
        let Some(upper) = self.store.last() else {
            return;
        };
        let mut index = upper.after(None);
        while let Some(current) = index {
            if let Some((_, slot)) = self.store.occupied_mut(current) {
                let mut state = topology::State::new(&mut slot.local, graph, values);
                f(&mut slot.channel, &mut state);
            }
            index = upper.after(Some(current));
        }
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Lease<'_, 0>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Lease<'_, 0>>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Table<'_, '_, '_, '_, 0>>() == 6 * mem::size_of::<usize>());
};

impl<const ID: u8> From<Lease<'_, ID>> for num::NonZero<u64> {
    fn from(value: Lease<'_, ID>) -> Self {
        value.packed
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Occupied<'_, 'r, 'c, 'b, 'd, ID> {
    fn get_mut(&mut self) -> &mut Slot<'r, 'c, 'b, 'd, ID> {
        // SAFETY: the slot value cannot change the retained table's bitmap storage or domain.
        unsafe { self.row.source_mut() }.store.value_mut(self.index)
    }

    fn vacate(mut self) {
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

// SAFETY: Table retains and exclusively owns the same Store and its immutable bitmap domain.
unsafe impl<const ID: u8> depot::raw::Words for Table<'_, '_, '_, '_, ID> {
    type Plane = ();
    type Size = u32;

    fn shape(&self) -> bitmap::Shape {
        self.store.shape()
    }

    fn words(&self, (): ()) -> &[bitmap::Word] {
        self.store.words(())
    }
}

const _: () = assert!(
    std::mem::size_of::<Occupied<'static, 'static, 'static, 'static, 'static, 0>>()
        == 3 * std::mem::size_of::<usize>()
);
