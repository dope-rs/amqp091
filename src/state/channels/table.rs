use std::{marker, num};

use dope::core::driver;
use o3::alloc;
use o3::depot::arena::slab;
use o3::depot::queue::resident;

use crate::failure::{self, wire};
use crate::permits;
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{self};
use crate::state::channel::{self, change, consumer};
use crate::state::channels::Index;
use crate::state::channels::map::{Generation, Map};
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

#[repr(transparent)]
pub(crate) struct Table<'r, 'c, 'b, 'd, const ID: u8> {
    table: Map<'r, 'c, 'b, 'd, ID>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Table<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn try_new(
        plan: Plan,
        driver: driver::Reference<'d>,
        permits: &permits::Registry<'c, 'd>,
    ) -> Result<Self, Error> {
        Ok(Self {
            table: Map::try_new(plan, driver, permits)?,
        })
    }

    pub(crate) fn acquire<R, const EXACT: bool>(
        &mut self,
        bound: u16,
        init: impl FnOnce(Index<'c, ID>, Lease<'d, ID>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        self.table.acquire::<R, EXACT>(bound, init)
    }

    pub(crate) fn lease(&self, id: u16) -> Option<Lease<'d, ID>> {
        self.table.key_at(id)
    }

    pub(crate) fn generation(&self) -> Option<Generation> {
        self.table.generation()
    }

    pub(crate) fn collect(&mut self, lease: Lease<'d, ID>) {
        let Some(mut entry) = self.table.occupied(lease) else {
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
        let Some(mut entry) = self.table.occupied(lease) else {
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
            let (index, lease, slot) = self.table.next(after)?;
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
        let (_, slot) = self.table.try_index_mut(lease)?;
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
        let (index, slot) = self.table.try_index_mut(lease).ok()?;
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
        self.table.visit(|slot| {
            let mut state = topology::State::new(&mut slot.local, graph, values);
            f(&mut slot.channel, &mut state);
        });
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Lease<'_, 0>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Lease<'_, 0>>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Table<'_, '_, '_, '_, 0>>() == 6 * mem::size_of::<usize>());
    assert!(mem::size_of::<Table<'_, '_, '_, '_, 0>>() == mem::size_of::<Map<'_, '_, '_, '_, 0>>());
};

impl<const ID: u8> From<Lease<'_, ID>> for num::NonZero<u64> {
    fn from(value: Lease<'_, ID>) -> Self {
        value.packed
    }
}
