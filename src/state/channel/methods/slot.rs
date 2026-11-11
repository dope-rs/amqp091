use std::{mem, ptr};

use dope::core::driver::schedule::ready::wake::{Waker, store};
use o3::depot;
use o3::depot::arena::slab::{self, entries, key};
use o3::depot::index::Initialized;
use o3::depot::queue::indexed::keyed;
use o3::depot::queue::links;

use crate::failure::terminal;
use crate::failure::wire::turn::{Method, Request};
use crate::protocol::{self, method};
use crate::state::channel::methods::{Entry, MethodKey, Plane, entry, flight, send};
use crate::state::channel::{self, consumer, methods};

type Queued<'a, 'c, 'b, 'd, P> = keyed::Slot<Slot<'a, 'c, 'b, 'd, P>, Plane>;

#[repr(C)]
struct Variant<T> {
    tag: u8,
    value: T,
}

impl<'b, 'd> Variant<entry::Pending<'b, 'd>> {
    fn read<'a, 'c>(
        live: depot::raw::Proof<&'a Entry<'c, 'b, 'd>, Initialized<Plane>>,
    ) -> &'a entry::Pending<'b, 'd> {
        // SAFETY: Plane retains the Pending variant; repr(u8) gives its repr(C) tag/payload layout.
        unsafe { &(*ptr::from_ref(&live.into_inner().state).cast::<Self>()).value }
    }
}

const _: () = {
    assert!(mem::offset_of!(Variant<entry::Pending<'_, '_>>, tag) == 0);
    assert!(
        mem::size_of::<Variant<entry::Pending<'_, '_>>>()
            <= mem::size_of::<entry::Stage<'_, '_, '_>>()
    );
    assert!(
        mem::align_of::<Variant<entry::Pending<'_, '_>>>()
            <= mem::align_of::<entry::Stage<'_, '_, '_>>()
    );
    assert!(
        mem::size_of::<Variant<entry::Ready<'_, '_, '_>>>()
            <= mem::size_of::<entry::Stage<'_, '_, '_>>()
    );
    assert!(
        mem::align_of::<Variant<entry::Ready<'_, '_, '_>>>()
            <= mem::align_of::<entry::Stage<'_, '_, '_>>()
    );
};

struct Answered<'a, 'c, 'b, 'd>(slab::Occupied<entries::Busy<'a, Entry<'c, 'b, 'd>>, Request>);

impl<'c, 'b, 'd> Answered<'_, 'c, 'b, 'd> {
    fn remove(self) -> entry::Ready<'c, 'b, 'd> {
        let Entry { state, .. } = self.0.remove();
        let state = mem::ManuallyDrop::new(state);
        // SAFETY: the first state classification retained this exact Ready variant and owner.
        unsafe {
            ptr::addr_of!(
                (*ptr::from_ref(&*state).cast::<Variant<entry::Ready<'c, 'b, 'd>>>()).value
            )
            .read()
        }
    }
}

pub(in crate::state::channel::methods) struct Slot<'a, 'c, 'b, 'd, P = ()> {
    pub(super) entry: slab::Occupied<
        entries::Busy<'a, Entry<'c, 'b, 'd>, { u32::MAX }, entries::Exclusive<false>, 1, P>,
        Request,
    >,
    pub(super) order: &'a mut keyed::Fifo<MethodKey, Plane>,
}

impl<'c, 'b, 'a, 'd> Slot<'a, 'c, 'b, 'd> {
    pub(super) fn try_new<const ID: u8>(
        owner: &'a mut methods::Methods<'c, 'b, 'd, ID>,
        key: MethodKey,
    ) -> Result<Self, key::Error<Request>> {
        let entry = owner.pending.try_entry(key.raw())?;
        Ok(Self {
            entry,
            order: &mut owner.order,
        })
    }

    pub(super) fn first<const ID: u8>(
        owner: &'a mut methods::Methods<'c, 'b, 'd, ID>,
    ) -> Option<MethodKey> {
        let (cursor, ..) = Self::front(owner)?;
        Some(cursor.key())
    }

    pub(super) fn head<const ID: u8>(
        owner: &'a methods::Methods<'c, 'b, 'd, ID>,
    ) -> Option<&'a entry::Pending<'b, 'd>> {
        let live = keyed::Fifo::first(owner)?;
        // SAFETY: projection retains the original root's shared storage borrow.
        let live = unsafe { live.map(|(owner, index)| (&owner.pending, index)) };
        let live = slab::Store::value(live);
        // SAFETY: the selected entry retains this exact FIFO's Pending plane.
        Some(Variant::read(unsafe { live.map(|(entry, _)| entry) }))
    }

    pub(super) fn front<const ID: u8>(
        owner: &'a mut methods::Methods<'c, 'b, 'd, ID>,
    ) -> Option<(
        Queued<'a, 'c, 'b, 'd, Plane>,
        &'a mut flight::Flight<'c, 'b, 'd, ID>,
        store::Store<'d>,
    )> {
        let live = keyed::Fifo::first(owner)?;
        Some(Self::select(live))
    }

    pub(super) fn select<const ID: u8>(
        live: depot::raw::Proof<
            (&'a mut methods::Methods<'c, 'b, 'd, ID>, links::Index),
            Initialized<Plane>,
        >,
    ) -> (
        Queued<'a, 'c, 'b, 'd, Plane>,
        &'a mut flight::Flight<'c, 'b, 'd, ID>,
        store::Store<'d>,
    ) {
        // SAFETY: splitting this original root retains the same storage, queue,
        // flight, and driver lifetimes alongside the initialized member proof.
        let live = unsafe {
            live.map(|(owner, index)| {
                (
                    (
                        &mut owner.pending,
                        (&mut owner.order, &mut owner.flight, owner.wakes),
                    ),
                    index,
                )
            })
        };
        let (entry, (order, flight, wakes)) = slab::Occupied::from_live(live);
        // SAFETY: the retained FIFO member belongs to this exact original root.
        let cursor =
            unsafe { keyed::Slot::from_source(depot::raw::Proof::new(Slot { entry, order })) };
        (cursor, flight, wakes)
    }

    pub(super) fn insert<const FRONT: bool>(
        vacant: methods::Vacant<'a, 'c, 'b, 'd>,
        build: impl FnOnce(links::Index) -> Entry<'c, 'b, 'd>,
    ) -> send::Next {
        let methods::Vacant { entry, order } = vacant;
        let entry = entry.stage_with(build).commit();
        send::Next::new(Self { entry, order }.push::<FRONT>().key())
    }

    pub(super) fn push<const FRONT: bool>(self) -> Queued<'a, 'c, 'b, 'd, ()> {
        // SAFETY: the insertion consumes the method owner's reservation. Each
        // builder initializes this slot's link with its supplied bounded index.
        let vacant =
            unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(self)) };
        vacant.push::<FRONT>()
    }

    pub(super) fn poll(
        self,
        wakes: store::Store<'d>,
        wake: Waker<'d>,
    ) -> Option<Result<protocol::Response<'b, 'd, consumer::Key>, terminal::Outcome<'c, 'b, 'd>>>
    {
        match &self.entry.get().state {
            entry::Stage::Pending(state) => {
                wakes.set(&state.waker, wake);
                None
            }
            entry::Stage::Ready(_) => Some(Answered(self.entry).remove().outcome),
        }
    }

    pub(super) fn detach(mut self, phase: &mut channel::Phase) -> Removal {
        match &mut self.entry.get_mut().state {
            entry::Stage::Ready(ready) => {
                let expected = ready.expect;
                let outcome = Answered(self.entry).remove().outcome;
                let action = match (expected, outcome) {
                    (method::Kind::Open, Ok(protocol::Response::Empty(method::Kind::Open)))
                        if *phase == channel::Phase::Open =>
                    {
                        methods::DetachAction::CloseAbandonedOpen
                    }
                    (_, Ok(protocol::Response::BasicConsumeOk(_, key))) => {
                        methods::DetachAction::RetireConsumer(key)
                    }
                    _ => methods::DetachAction::None,
                };
                Removal::Ready(action)
            }
            entry::Stage::Pending(state) => {
                if state.sent {
                    state.origin = entry::Origin::Detached;
                    state.waker.clear();
                    return Removal::Retained;
                }
                if state.expect == method::Kind::Open {
                    *phase = channel::Phase::Closed;
                } else if state.expect == method::Kind::CloseOk {
                    *phase = channel::Phase::Open;
                }
                // SAFETY: the retained Pending state was inserted into this exact FIFO.
                let cursor = unsafe { keyed::Slot::from_source(depot::raw::Proof::new(self)) };
                cursor.remove().release().discard();
                Removal::Unlinked
            }
        }
    }
}

pub(in crate::state::channel::methods) enum Removal {
    Ready(methods::DetachAction),
    Retained,
    Unlinked,
}

// SAFETY: Slot retains the method root's exclusive reservation/slot borrow and
// its sole order plane. Pending storage cannot be edited while this owner lives.
unsafe impl<'c, 'b, 'a, 'd, P> depot::raw::KeyFinish<true, Plane> for Slot<'a, 'c, 'b, 'd, P> {
    type Output = methods::Member<'a, 'c, 'b, 'd, P>;

    unsafe fn finish(self) -> Self::Output {
        Member(self)
    }
}

unsafe impl<'c, 'b, 'a, 'd, P> depot::raw::KeyFinish<false, Plane> for Slot<'a, 'c, 'b, 'd, P> {
    type Output = Detached<'a, 'c, 'b, 'd, P>;

    unsafe fn finish(self) -> Self::Output {
        Detached { slot: self.entry }
    }
}

unsafe impl<P> depot::raw::KeyOwner<Plane> for Slot<'_, '_, '_, '_, P> {
    type Key = MethodKey;
    type Fifo = keyed::Fifo<MethodKey, Plane>;

    fn parts(&self) -> (MethodKey, &keyed::Link<MethodKey, Plane>) {
        (
            MethodKey::from_raw(self.entry.key()),
            &self.entry.get().link,
        )
    }

    fn fifo(&self) -> &keyed::Fifo<MethodKey, Plane> {
        self.order
    }

    fn link(
        live: depot::raw::Proof<(&Self, links::Index), Initialized<Plane>>,
    ) -> &keyed::Link<MethodKey, Plane> {
        // SAFETY: the retained native guard owns this live neighbor's exact slab.
        let live = unsafe { live.map(|(slot, index)| (&slot.entry, index)) };
        let (_, entry) = slab::Occupied::project(live);
        &entry.link
    }
}

/// An unlinked method retains its initialized slot and Pending plane.
pub(in crate::state::channel::methods) struct Detached<'a, 'c, 'b, 'd, P = ()> {
    slot: slab::Occupied<
        entries::Busy<'a, Entry<'c, 'b, 'd>, { u32::MAX }, entries::Exclusive<false>, 1, P>,
        Request,
    >,
}

const _: () = assert!(mem::size_of::<Detached<'_, '_, '_, '_>>() == 2 * mem::size_of::<usize>());

impl<'c, 'b, 'd, P> Detached<'_, 'c, 'b, 'd, P> {
    pub(super) fn discard(self) {
        self.slot.discard();
    }

    pub(super) fn reply(
        self,
        outcome: Result<protocol::Response<'b, 'd, consumer::Key>, terminal::Outcome<'c, 'b, 'd>>,
        wakes: store::Store<'d>,
    ) {
        // SAFETY: consuming unlink retained this slot's Pending variant and owner.
        let pending = Variant::read(unsafe { depot::raw::Proof::new(self.slot.get()) });
        if pending.origin == entry::Origin::User {
            let expect = pending.expect;
            // SAFETY: this exclusive slot transfers its wake entry before overwrite.
            let waker = unsafe { ptr::read(&pending.waker) };
            // SAFETY: User retains Outbound::None; the other Pending fields have
            // no Drop. Publish Ready in the same initialized slot before waking.
            unsafe {
                ptr::write(
                    &mut self.slot.into_mut().state,
                    entry::Stage::Ready(entry::Ready { expect, outcome }),
                );
            }
            wakes.wake_once(&waker);
        } else {
            self.slot.discard();
        }
    }
}

/// A queued method whose payload can be borrowed without exposing its link.
pub(in crate::state::channel::methods) struct Member<'a, 'c, 'b, 'd, P = ()>(
    Slot<'a, 'c, 'b, 'd, P>,
);

impl<'c, 'b, 'a, 'd, P> Member<'a, 'c, 'b, 'd, P> {
    pub(super) fn get(&self) -> &entry::Pending<'b, 'd> {
        // SAFETY: Member retains this exact queued Pending variant and its owner.
        Variant::read(unsafe { depot::raw::Proof::new(self.0.entry.get()) })
    }

    pub(super) fn remove(self) -> Detached<'a, 'c, 'b, 'd, P> {
        // SAFETY: Member retains membership in this exact exclusively borrowed FIFO.
        keyed::Slot::from_source(unsafe { depot::raw::Proof::new(self.0) })
            .remove()
            .release()
    }

    pub(super) fn send(self) -> Result<&'a mut entry::Pending<'b, 'd>, Method> {
        self.pending().send()
    }

    pub(super) fn pending(self) -> &'a mut entry::Pending<'b, 'd> {
        let entry = self.0.entry.into_mut();
        // SAFETY: Member retains the queued Pending variant and the original exclusive owner lifetime.
        unsafe {
            &mut (*ptr::from_mut(&mut entry.state).cast::<Variant<entry::Pending<'b, 'd>>>()).value
        }
    }
}
