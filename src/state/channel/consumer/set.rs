use dope::core::driver::schedule::ready::wake;
use dope::core::driver::{self};
use o3::alloc;
use o3::depot::arena::hash::runtime;
use wake::store;

use crate::failure::wire::turn::Consumer;
use crate::protocol::topology::records::consumer::Record;
use crate::protocol::topology::records::queue;
use crate::protocol::wire::text::short;
use crate::state::channel::consumer::delivery::Store;
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::{self, consumer};

pub(crate) struct Set<'b, 'd, const ID: u8> {
    pub(super) wakes: store::Store<'d>,
    pub(super) slots: runtime::Registry<Slot<'b, 'd, ID>, consumer::Tag>,
}

impl<'b, 'd, const ID: u8> Set<'b, 'd, ID> {
    pub(crate) fn try_new(
        plan: consumer::Plan,
        driver: driver::Reference<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            wakes: store::Store::new(driver),
            slots: runtime::Registry::try_new(plan.resident)?,
        })
    }

    pub(crate) fn close(&mut self) {
        for index in 0..self.slots.capacity() {
            let Some(entry) = self.slots.entry_at(index) else {
                continue;
            };
            self.wakes.wake_once(&entry.get().waker);
            entry.discard();
        }
    }

    #[must_use = "consumer removal can free a blocked route"]
    pub(crate) fn detach<'r, 'c>(
        &mut self,
        key: consumer::Key,
        store: &'r Store<'c, 'b, 'd>,
    ) -> channel::Freed {
        let Some(mut entry) = self.slots.entry(key.raw()) else {
            return channel::Freed::Available;
        };
        let freed = store.drain(&mut entry.get_mut().queue);
        entry.discard();
        freed
    }

    pub(crate) fn rollback(&mut self, undo: Option<consumer::Rollback>) {
        let Some(undo) = undo else {
            return;
        };
        match undo {
            consumer::Rollback::New(key) => {
                let Some(entry) = self.slots.entry(key.raw()) else {
                    return;
                };
                if entry.get().status == Consumer::Pending {
                    entry.discard();
                }
            }
            consumer::Rollback::Replay(key) => {
                let Some(mut entry) = self.slots.entry(key.raw()) else {
                    return;
                };
                if entry.get().status == Consumer::Replaying {
                    entry.get_mut().status = Consumer::Recovering;
                }
            }
        }
    }

    pub(crate) fn forget_queue(
        &mut self,
        queue: Option<queue::Key<'d, ID>>,
        name: short::Short<'_>,
    ) {
        forget_if(&mut self.slots, |spec| match (spec.key(), queue) {
            (Some(left), Some(right)) => left == right,
            (None, _) => spec.name().value() == name.value(),
            _ => false,
        })
    }

    pub(crate) fn forget_key(&mut self, queue: queue::Key<'d, ID>) {
        forget_if(&mut self.slots, |spec| spec.key() == Some(queue));
    }

    pub(crate) fn disconnect(&mut self) {
        for index in 0..self.slots.capacity() {
            let Some(mut entry) = self.slots.entry_at(index) else {
                continue;
            };
            match entry.get().status {
                Consumer::Active => {
                    entry.get_mut().status = Consumer::Recovering;
                }
                Consumer::Untracked | Consumer::Replaying => {
                    let mut staged = entry.unindex();
                    staged.get_mut().status = Consumer::Canceled;
                    self.wakes.wake_once(&staged.get_mut().waker);
                    continue;
                }
                Consumer::Pending => {
                    entry.discard();
                    continue;
                }
                Consumer::Recovering | Consumer::Canceled | Consumer::Retiring => {}
            }
            self.wakes.wake_once(&entry.get_mut().waker);
        }
    }
}

fn forget_if<'b, 'd, const ID: u8>(
    set: &mut runtime::Registry<Slot<'b, 'd, ID>, consumer::Tag>,
    mut pick: impl FnMut(&Record<'b, 'd, ID>) -> bool,
) {
    for index in 0..set.capacity() {
        let Some(mut entry) = set.entry_at(index) else {
            continue;
        };
        if !pick(&entry.get().spec) {
            continue;
        }
        match entry.get().status {
            Consumer::Active => {
                entry.get_mut().status = Consumer::Untracked;
            }
            Consumer::Recovering | Consumer::Replaying => {
                let mut staged = entry.unindex();
                staged.get_mut().status = Consumer::Canceled;
            }
            Consumer::Pending | Consumer::Untracked | Consumer::Canceled | Consumer::Retiring => {}
        }
    }
}
