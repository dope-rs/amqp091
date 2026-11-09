use dope::core::driver::schedule::ready::wake;
use dope::core::driver::{self};
use o3::alloc;
use wake::store;

use crate::protocol::topology::records::consumer::Record;
use crate::protocol::topology::records::queue;
use crate::protocol::wire::text::short;
use crate::state::channel::consumer::access::{Read, Write};
use crate::state::channel::consumer::delivery::{Claim, Next, Route, Store};
use crate::state::channel::consumer::storage::registry::Registry;
use crate::state::channel::consumer::storage::status::Status;
use crate::state::channel::{self, consumer};

pub(crate) struct Set<'b, 'd, const ID: u8> {
    wakes: store::Store<'d>,
    registry: Registry<'b, 'd, ID>,
}

impl<'b, 'd, const ID: u8> Set<'b, 'd, ID> {
    pub(crate) fn try_new(
        plan: consumer::Plan,
        driver: driver::Reference<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            wakes: store::Store::new(driver),
            registry: Registry::try_new(plan)?,
        })
    }

    pub(crate) fn drain<'r, 'c>(&mut self, store: &'r Store<'c, 'b, 'd>) -> channel::Freed {
        self.registry.clear(store)
    }

    pub(crate) fn close(&mut self) {
        for index in 0..self.registry.slots.capacity() {
            let Some(entry) = self.registry.slots.entry_at(index) else {
                continue;
            };
            self.wakes.wake_once(&entry.get().waker);
            entry.discard();
        }
    }

    #[must_use = "consumer retirement can free a blocked route"]
    pub(crate) fn retire<'r, 'c>(
        &mut self,
        key: consumer::Key,
        store: &'r Store<'c, 'b, 'd>,
        replay: impl FnOnce() -> bool,
    ) -> Option<channel::Freed> {
        let (slot, freed) = self.registry.begin_retire(key, store, replay)?;
        slot.waker.clear();
        Some(freed)
    }

    #[must_use = "consumer removal can free a blocked route"]
    pub(crate) fn detach<'r, 'c>(
        &mut self,
        key: consumer::Key,
        store: &'r Store<'c, 'b, 'd>,
    ) -> channel::Freed {
        let Some((slot, freed)) = self.registry.remove(key, store) else {
            return channel::Freed::Available;
        };
        slot.waker.clear();
        freed
    }

    pub(crate) fn cancel<'r, 'c>(
        &mut self,
        tag: &str,
        store: &'r Store<'c, 'b, 'd>,
    ) -> channel::Freed {
        let Some((slot, freed)) = self.registry.cancel(tag, store) else {
            return channel::Freed::Available;
        };
        self.wakes.wake_once(&slot.waker);
        freed
    }

    pub(crate) fn next<'r, 'c>(
        &mut self,
        key: consumer::Key,
        wake: wake::Waker<'d>,
        store: &'r Store<'c, 'b, 'd>,
    ) -> Next<'c, 'b, 'd> {
        self.registry.next(store, self.wakes, key, wake)
    }

    pub(crate) fn reserve<'r, 'c>(
        &mut self,
        tag: &str,
        store: &'r Store<'c, 'b, 'd>,
    ) -> Option<Route<'r, 'c, 'b, 'd>> {
        self.registry.reserve(store, tag)
    }

    pub(crate) fn route<'r, 'c>(&mut self, claim: Claim<'r, 'c, 'b, 'd>) {
        self.registry.route(claim, self.wakes);
    }

    pub(crate) fn rollback(&mut self, undo: Option<consumer::Rollback>) {
        let Some(undo) = undo else {
            return;
        };
        match undo {
            consumer::Rollback::New(key) => {
                let Some(entry) = self.registry.slots.entry(key.raw()) else {
                    return;
                };
                if entry.get().status == Status::Pending {
                    entry.discard();
                }
            }
            consumer::Rollback::Replay(key) => {
                let Some(mut entry) = self.registry.slots.entry(key.raw()) else {
                    return;
                };
                if entry.get().status == Status::Replaying {
                    entry.get_mut().status = Status::Recovering;
                }
            }
        }
    }

    pub(crate) fn forget_queue(
        &mut self,
        queue: Option<queue::Key<'d, ID>>,
        name: short::Short<'_>,
    ) {
        forget_if(&mut self.registry, |spec| match (spec.key(), queue) {
            (Some(left), Some(right)) => left == right,
            (None, _) => spec.name().value() == name.value(),
            _ => false,
        })
    }

    pub(crate) fn forget_key(&mut self, queue: queue::Key<'d, ID>) {
        forget_if(&mut self.registry, |spec| spec.key() == Some(queue));
    }

    pub(crate) fn disconnect(&mut self) {
        for index in 0..self.registry.slots.capacity() {
            let Some(mut entry) = self.registry.slots.entry_at(index) else {
                continue;
            };
            match entry.get().status {
                Status::Active => {
                    entry.get_mut().status = Status::Recovering;
                }
                Status::Untracked | Status::Replaying => {
                    let mut staged = entry.unindex();
                    staged.get_mut().status = Status::Canceled;
                    self.wakes.wake_once(&staged.get_mut().waker);
                    continue;
                }
                Status::Pending => {
                    entry.discard();
                    continue;
                }
                Status::Recovering | Status::Canceled | Status::Retiring => {}
            }
            self.wakes.wake_once(&entry.get_mut().waker);
        }
    }

    pub(crate) const fn read(&self) -> Read<'_, 'b, 'd, ID> {
        Read::new(&self.registry)
    }

    pub(crate) fn write(&mut self) -> Write<'_, 'b, 'd, ID> {
        Write::new(&mut self.registry)
    }
}

fn forget_if<'b, 'd, const ID: u8>(
    set: &mut Registry<'b, 'd, ID>,
    mut pick: impl FnMut(&Record<'b, 'd, ID>) -> bool,
) {
    for index in 0..set.slots.capacity() {
        let Some(mut entry) = set.slots.entry_at(index) else {
            continue;
        };
        if !pick(&entry.get().spec) {
            continue;
        }
        match entry.get().status {
            Status::Active => {
                entry.get_mut().status = Status::Untracked;
            }
            Status::Recovering | Status::Replaying => {
                let mut staged = entry.unindex();
                staged.get_mut().status = Status::Canceled;
            }
            Status::Pending | Status::Untracked | Status::Canceled | Status::Retiring => {}
        }
    }
}
