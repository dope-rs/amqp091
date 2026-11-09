use std::{mem, num};

use dope::core::driver::schedule::ready::wake::table::raw::Index as _;
use dope::core::driver::schedule::ready::wake::{self, table};
use dope::core::driver::{self};
use o3::base::num::bounded;
use o3::depot::arena::linked::{fifo, slots};
use o3::depot::{self, capacity, queue::resident};

use crate::adapter::channel::Returned;
use crate::state::channel::{Freed, Phase};

pub(super) enum Tag {}

pub(crate) struct Queue {
    pub(super) queue: fifo::Fifo<Tag>,
    slot: bounded::U32<0, { u32::MAX - 1 }>,
}

struct Source<'a, 'r, 'd> {
    queue: &'a mut Queue,
    table: &'r table::Table<'d>,
}

struct Index<'scope> {
    slot: bounded::U32<0, { u32::MAX - 1 }>,
    _scope: table::Scope<'scope>,
}

pub(crate) enum Next<'c, 'b, 'd> {
    Pending,
    Closed,
    Item {
        returned: Returned<'c, 'b, 'd>,
        freed: Freed,
    },
}

impl Queue {
    pub(crate) fn new(position: num::NonZero<u16>) -> Self {
        Self {
            queue: fifo::Fifo::new(),
            slot: bounded::U32::from_one(position.into()),
        }
    }

    pub(crate) fn next<'r, 'c, 'b, 'd>(
        &mut self,
        phase: Phase,
        wait: &mut table::Registration<'r, 'd>,
        wake: wake::Waker<'d>,
        store: &'r Store<'c, 'b, 'd>,
    ) -> Result<Next<'c, 'b, 'd>, table::Error> {
        if phase != Phase::Open {
            return Ok(Next::Closed);
        }
        table::Table::with_source(
            Source {
                queue: self,
                table: &store.wakes,
            },
            |queue, scope, sink| {
                Index {
                    slot: queue.slot,
                    _scope: scope,
                }
                .try_register(&sink, wait, &wake)?;
                let (returned, freed) = store.pop(queue);
                Ok(match returned {
                    Some(returned) => Next::Item { returned, freed },
                    None => Next::Pending,
                })
            },
        )
    }

    pub(crate) fn wake(&mut self, store: &Store<'_, '_, '_>) {
        table::Table::with_source(
            Source {
                queue: self,
                table: &store.wakes,
            },
            |queue, scope, sink| {
                Index {
                    slot: queue.slot,
                    _scope: scope,
                }
                .wake(&sink);
            },
        );
    }
}

// SAFETY: channel initialization admits each physical position against the same
// Plan that fixes this table's extent. Channel storage retains this pairing.
unsafe impl<'a, 'r: 'a, 'd: 'r> table::raw::Source<'a, 'r, 'd> for Source<'a, 'r, 'd> {
    type State = Queue;

    fn parts(self) -> (&'a mut Self::State, &'r table::Table<'d>) {
        (self.queue, self.table)
    }
}

// SAFETY: only the paired source brands its retained, admitted physical slot.
unsafe impl<'scope> table::raw::Index<'scope> for Index<'scope> {
    fn index(self) -> bounded::U32<0, { u32::MAX - 1 }> {
        self.slot
    }
}

const _: () = assert!(mem::size_of::<Queue>() == 4 * mem::size_of::<u32>());

pub(crate) struct Store<'c, 'b, 'd> {
    nodes: depot::raw::Arena<Returned<'c, 'b, 'd>, Tag>,
    wakes: table::Table<'d>,
}

#[repr(transparent)]
pub(crate) struct Vacant<'r, 'c, 'b, 'd>(
    slots::Vacant<Returned<'c, 'b, 'd>, &'r slots::Store<Returned<'c, 'b, 'd>>, Tag>,
);

#[repr(transparent)]
pub(crate) struct Claim<'r, 'c, 'b, 'd>(
    slots::Live<Returned<'c, 'b, 'd>, &'r slots::Store<Returned<'c, 'b, 'd>>, Tag>,
);

impl<'c, 'b, 'd> Store<'c, 'b, 'd> {
    pub(crate) fn try_new(
        capacity: capacity::Count,
        channels: usize,
        driver: driver::Reference<'d>,
    ) -> Result<Self, resident::Error> {
        Ok(Self {
            nodes: depot::raw::Arena::try_new(capacity.get())?,
            wakes: table::Table::try_new(driver, channels)?,
        })
    }

    pub(crate) fn vacant<'r>(&'r self) -> Option<Vacant<'r, 'c, 'b, 'd>> {
        self.nodes.vacant().map(Vacant)
    }

    fn queue<'a>(&'a self, queue: &'a mut Queue) -> slots::Bound<'a, Returned<'c, 'b, 'd>, Tag> {
        // SAFETY: channel storage pairs this queue with its original return nodes.
        unsafe { self.nodes.queue(&mut queue.queue) }
    }

    fn pop(&self, queue: &mut Queue) -> (Option<Returned<'c, 'b, 'd>>, Freed) {
        let was_full = self.nodes.available() == 0;
        Freed::popped(was_full, self.queue(queue).pop_front())
    }

    pub(crate) fn drain(&self, queue: &mut Queue) -> Freed {
        let was_full = self.nodes.available() == 0;
        let mut queue = self.queue(queue);
        let (mut next, freed) = Freed::popped(was_full, queue.pop_front());
        while next.is_some() {
            next = queue.pop_front();
        }
        freed
    }
}

impl<'r, 'c, 'b, 'd> Vacant<'r, 'c, 'b, 'd> {
    pub(crate) fn insert(self, returned: Returned<'c, 'b, 'd>) -> Claim<'r, 'c, 'b, 'd> {
        Claim(self.0.insert(returned))
    }
}

impl<'r, 'c, 'b, 'd> Claim<'r, 'c, 'b, 'd> {
    pub(crate) fn push(self, queue: &mut Queue, store: &Store<'c, 'b, 'd>) {
        slots::Live::push_back(unsafe { depot::raw::Proof::new((self.0, &mut queue.queue)) });
        queue.wake(store);
    }
}

const _: () = {
    assert!(
        mem::size_of::<Store<'_, '_, '_>>()
            == mem::size_of::<depot::raw::Arena<Returned<'_, '_, '_>, Tag>>()
                + mem::size_of::<table::Table<'_>>()
    );
    assert!(mem::size_of::<Vacant<'_, '_, '_, '_>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Claim<'_, '_, '_, '_>>() == 2 * mem::size_of::<usize>());
};
