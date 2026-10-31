use o3::depot::arena::slab;
use o3::depot::arena::slab::entries;
use o3::depot::queue::indexed::keyed;

use crate::failure::wire::turn;
use crate::protocol::wire::text::resident;
use crate::state::channel::consumer;
use crate::state::channel::methods::{Entry, MethodKey, Plane, Slot, entry, send};

/// An exact method-storage vacancy whose queue link lives in the inserted operation.
#[must_use = "dropping a vacancy releases its reservation"]
pub(crate) struct Vacant<'a, 'c, 'b, 'd> {
    pub(super) entry: slab::Vacant<entries::Pending<'a, Entry<'c, 'b, 'd>>, turn::Request>,
    pub(super) order: &'a mut keyed::Fifo<MethodKey, Plane>,
}

impl<'c, 'b, 'a, 'd> Vacant<'a, 'c, 'b, 'd> {
    pub(super) fn new(
        entry: slab::Vacant<entries::Pending<'a, Entry<'c, 'b, 'd>>, turn::Request>,
        order: &'a mut keyed::Fifo<MethodKey, Plane>,
    ) -> Self {
        Self { entry, order }
    }

    pub(crate) fn retire(self, consumer: consumer::Key, ready: bool) -> Option<send::Next> {
        let key = Slot::insert::<false>(self, |key| {
            Entry::new(key, entry::Pending::consumer(consumer))
        })
        .key();
        ready.then_some(send::Next::new(key))
    }

    pub(crate) fn abandon(self, consumer: consumer::Key, blocked: bool) -> Option<send::Next> {
        let key = Slot::insert::<true>(self, |key| {
            Entry::new(key, entry::Pending::consumer(consumer))
        })
        .key();
        (!blocked).then_some(send::Next::new(key))
    }

    pub(crate) fn orphan(self, tag: resident::Short<'b, 'd>, blocked: bool) -> Option<send::Next> {
        let key = Slot::insert::<true>(self, |key| {
            Entry::new(key, entry::Pending::orphan(tag, entry::Origin::Detached))
        })
        .key();
        (!blocked).then_some(send::Next::new(key))
    }

    pub(crate) fn retry(self, tag: resident::Short<'b, 'd>, blocked: bool) -> Option<send::Next> {
        let key = Slot::insert::<true>(self, |key| {
            Entry::new(key, entry::Pending::orphan(tag, entry::Origin::Replay))
        })
        .key();
        (!blocked).then_some(send::Next::new(key))
    }

    pub(crate) fn close(self) -> send::Next {
        send::Next::new(
            Slot::insert::<true>(self, |key| Entry::new(key, entry::Pending::close())).key(),
        )
    }
}
