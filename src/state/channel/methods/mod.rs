use std::{mem, num};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::{self};
pub(crate) use entry::Entry;
use o3::depot::arena::slab::{self, key};
use o3::depot::capacity;
use o3::depot::queue::indexed::keyed;
use o3::{alloc, depot};
pub(crate) use permit::Permit;
pub(crate) use vacant::Vacant;
use wake::{Waker, store};

use crate::failure::wire::turn::{Method, Request};
use crate::failure::{self, terminal, wire};
use crate::protocol::wire::text::resident;
use crate::protocol::{self, method};
use crate::state::channel::{self, consumer, methods};

mod entry;
pub(crate) mod flight;
mod permit;
mod raw;
pub(crate) mod send;
mod slot;
pub(crate) mod turn;
mod vacant;
pub(in crate::state::channel::methods) use slot::{Member, Removal, Slot};

enum Plane {}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MethodKey(key::Handle<Request>);

impl MethodKey {
    const fn from_raw(raw: key::Handle<Request>) -> Self {
        Self(raw)
    }

    pub(crate) const fn raw(self) -> key::Handle<Request> {
        self.0
    }

    pub(crate) const fn index(self) -> u32 {
        self.0.index()
    }
}

pub(crate) enum DetachAction {
    None,
    RetireConsumer(consumer::Key),
    CloseAbandonedOpen,
}

pub(crate) struct DetachResult {
    pub action: DetachAction,
    pub control: Option<send::Next>,
}

#[must_use = "method completion can free a blocked content route"]
pub(crate) enum Next<'b, 'd> {
    Idle {
        freed: channel::Freed,
    },
    Resume {
        next: send::Next,
        freed: channel::Freed,
    },
    Replay {
        freed: channel::Freed,
    },
    Close {
        freed: channel::Freed,
    },
    Cancel {
        cancel: consumer::Cancel<'b, 'd>,
        freed: channel::Freed,
    },
    ReplayCancel {
        cancel: resident::Short<'b, 'd>,
        freed: channel::Freed,
    },
}

pub(crate) enum Head {
    Empty,
    Waiting,
    Sent(method::Kind),
}

pub(crate) struct Methods<'c, 'b, 'd, const ID: u8> {
    wakes: store::Store<'d>,
    pending: slab::Store<Entry<'c, 'b, 'd>, Request>,
    order: keyed::Fifo<MethodKey, Plane>,
    flight: flight::Flight<'c, 'b, 'd, ID>,
    user_capacity: capacity::Count,
    storage_capacity: num::NonZero<u32>,
}

impl<'c, 'b, 'd, const ID: u8> Methods<'c, 'b, 'd, ID> {
    pub(crate) fn try_new(
        user: capacity::Count,
        store: num::NonZero<u32>,
        driver: driver::Reference<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            wakes: store::Store::new(driver),
            pending: slab::Store::try_new(capacity::Count::new(store.get()))?,
            order: keyed::Fifo::new(),
            flight: flight::Flight::new(),
            user_capacity: user,
            storage_capacity: store,
        })
    }

    pub(crate) fn head(&self) -> Head {
        match Slot::head(self) {
            Some(entry) if entry.sent => Head::Sent(entry.expect),
            Some(_) => Head::Waiting,
            None => Head::Empty,
        }
    }

    pub(crate) fn sent(&mut self) -> Option<(MethodKey, method::Kind)> {
        let (cursor, ..) = Slot::front(self)?;
        let key = cursor.key();
        let state = cursor.release().pending();
        state.sent.then_some((key, state.expect))
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.pending.len() == capacity::Count::EMPTY && self.flight.is_empty()
    }

    pub(crate) fn replays(&self, key: consumer::Key) -> bool {
        self.flight.replays(key)
    }

    pub(crate) fn vacant_user(&mut self, key: consumer::Key) -> Option<Vacant<'_, 'c, 'b, 'd>> {
        // Both callers retain a validated key from this channel's fixed consumer capacity.
        // Limits admitted user + consumers + 1 before allocating these slots.
        let index = self.user_capacity.raw() + key.index();
        let entry = self.pending.vacant_at(index)?;
        Some(Vacant::new(entry, &mut self.order))
    }

    pub(crate) fn retire(
        &mut self,
        key: consumer::Key,
        closing: bool,
        busy: bool,
    ) -> Option<Option<send::Next>> {
        if !closing {
            let ready = self.order.is_empty() && !busy;
            return Some(self.vacant_user(key)?.retire(key, ready));
        }
        // Wire-open Closing retains an unsent Close at the tail.
        let Some(mut close) = keyed::Fifo::last(&mut *self) else {
            unreachable!("close")
        };
        // SAFETY: append retains the old Close's initialized slot, membership,
        // and index in this exact fixed slab and FIFO.
        let _ = unsafe { close.as_mut() }
            .into_inner()
            .0
            .vacant_user(key)?
            .retire(key, false);
        Slot::select(close).0.remove().push::<false>();
        Some(if busy { None } else { self.next::<false>().1 })
    }

    pub(crate) fn vacant(&mut self) -> Option<Vacant<'_, 'c, 'b, 'd>> {
        let index = self.storage_capacity.get() - 1;
        let entry = self.pending.vacant_at(index)?;
        Some(Vacant::new(entry, &mut self.order))
    }

    pub(crate) fn replay(
        &mut self,
        expect: method::Kind,
    ) -> Result<Permit<'_, 'c, 'b, 'd, ID>, failure::Error> {
        let front = Slot::first(self);
        let index = self.storage_capacity.get() - 1;
        let entry = self
            .pending
            .vacant_at(index)
            .ok_or_else(|| failure::Error::Capacity(wire::Capacity::MethodReplay))?;
        let staged = entry.stage_with(|key| Entry::new(key, entry::Pending::replay(expect)));
        let key = MethodKey::from_raw(staged.key());
        if let Some(actual) = front {
            return Err(failure::Error::Protocol(wire::Fault::MethodOrder {
                expected: num::NonZero::from(key),
                actual: Some(num::NonZero::from(actual)),
            }));
        }
        self.flight.vacant(key).map_err(failure::Error::Protocol)?;
        let cursor = Slot {
            entry: staged.commit(),
            order: &mut self.order,
        }
        .push::<false>();
        let state = cursor.release().pending();
        Ok(Permit::new(key, state, &mut self.flight))
    }

    pub(crate) fn enqueue<R>(
        &mut self,
        phase: &mut channel::Phase,
        expect: method::Kind,
        admit: impl FnOnce(MethodKey) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        let next = match (*phase, expect) {
            (channel::Phase::Closed, method::Kind::Open) => Some(channel::Phase::Opening),
            (channel::Phase::Open, _) => None,
            _ => return Err(failure::Error::Closed),
        };
        let entry = self
            .pending
            .vacant_in(0..self.user_capacity.raw())
            .ok_or_else(|| failure::Error::Capacity(wire::Capacity::Method))?;
        let staged = entry.stage_with(|key| Entry::new(key, entry::Pending::user(expect)));
        let admitted = admit(MethodKey::from_raw(staged.key()))?;
        Slot {
            entry: staged.commit(),
            order: &mut self.order,
        }
        .push::<false>();
        if let Some(next) = next {
            *phase = next;
        }
        Ok(admitted)
    }

    pub(crate) fn claim(
        &mut self,
        channel: u16,
        next: &send::Next,
    ) -> Result<send::Pending<'_, 'b, 'd>, wire::Fault> {
        let expected = next.key();
        let (cursor, _, _) = Slot::front(self).ok_or(wire::Fault::ControlMethod {
            channel,
            expected: num::NonZero::from(expected),
            actual: None,
        })?;
        let actual = cursor.key();
        if actual != expected {
            return Err(wire::Fault::ControlMethod {
                channel,
                expected: num::NonZero::from(expected),
                actual: Some(num::NonZero::from(actual)),
            });
        }
        let state = cursor
            .release()
            .send()
            .map_err(|actual| wire::Fault::MethodState {
                method: Some(num::NonZero::from(expected)),
                expected: Method::Control,
                actual,
            })?;
        Ok(send::Pending::new(state))
    }

    pub(crate) fn next<const REPLAY: bool>(&mut self) -> (bool, Option<send::Next>) {
        let Some((cursor, _, wakes)) = Slot::front(self) else {
            return (false, None);
        };
        let key = cursor.key();
        let state = cursor.release().pending();
        let ctrl = state.is_control();
        if REPLAY && ctrl {
            state.origin = entry::Origin::Replay;
        }
        if state.sent {
            return (true, None);
        }
        if !ctrl {
            wakes.wake_once(&state.waker);
            return (true, None);
        }
        (true, Some(send::Next::new(key)))
    }

    pub(crate) fn reserve<Text>(
        &mut self,
        key: MethodKey,
        active: bool,
    ) -> Result<Option<Permit<'_, 'c, 'b, 'd, ID>>, failure::Error<Text>> {
        let front = Slot::first(self);
        let entry = self
            .pending
            .try_entry(key.raw())
            .map_err(failure::Error::Method)?;
        let entry = entry.into_mut();
        let state = entry.waiting().map_err(|actual| {
            failure::Error::Protocol(wire::Fault::MethodState {
                method: Some(num::NonZero::from(key)),
                expected: Method::Waiting,
                actual,
            })
        })?;
        let front = front.ok_or_else(|| {
            failure::Error::Protocol(wire::Fault::MethodOrder {
                expected: num::NonZero::from(key),
                actual: None,
            })
        })?;
        if front != key || active {
            return Ok(None);
        }
        self.flight.vacant(key).map_err(failure::Error::Protocol)?;
        Ok(Some(Permit::new(key, state, &mut self.flight)))
    }

    pub(crate) fn register(
        &mut self,
        key: MethodKey,
        wake: Waker<'d>,
    ) -> Result<(), failure::Error> {
        let entry = self
            .pending
            .try_entry(key.raw())
            .map_err(failure::Error::Method)?;
        let entry = entry.into_mut();
        let state = entry.get_mut().map_err(|actual| {
            failure::Error::Protocol(wire::Fault::MethodState {
                method: Some(num::NonZero::from(key)),
                expected: Method::Pending,
                actual,
            })
        })?;
        self.wakes.set(&state.waker, wake);
        Ok(())
    }

    pub(crate) fn poll(
        &mut self,
        key: MethodKey,
        wake: Waker<'d>,
    ) -> Result<
        Option<Result<protocol::Response<'b, 'd, consumer::Key>, terminal::Outcome<'c, 'b, 'd>>>,
        failure::Error,
    > {
        let wakes = self.wakes;
        let slot = Slot::try_new(self, key).map_err(failure::Error::Method)?;
        Ok(slot.poll(wakes, wake))
    }

    pub(crate) fn reap(
        &mut self,
        phase: &mut channel::Phase,
        active: bool,
        id: num::NonZero<u64>,
    ) -> Result<DetachResult, failure::Error> {
        let parts = key::Parts::from_raw(id).ok_or_else(|| failure::Error::MethodIdentity(id))?;
        let key = MethodKey::from_raw(key::Handle::from_parts(parts));
        let front = Slot::first(self) == Some(key);
        let slot = Slot::try_new(self, key).map_err(failure::Error::Method)?;
        Ok(match slot.detach(phase) {
            Removal::Ready(action) => DetachResult {
                action,
                control: None,
            },
            Removal::Retained => DetachResult {
                action: DetachAction::None,
                control: None,
            },
            Removal::Unlinked => DetachResult {
                action: DetachAction::None,
                control: if front && !active {
                    self.next::<false>().1
                } else {
                    None
                },
            },
        })
    }
}

const _: () = {
    let word = mem::size_of::<usize>();

    assert!(
        mem::size_of::<Next<'static, 'static>>()
            == mem::size_of::<consumer::Cancel<'static, 'static>>() + word
    );
    assert!(
        mem::size_of::<Option<Next<'static, 'static>>>()
            == mem::size_of::<Next<'static, 'static>>()
    );
    assert!(mem::size_of::<Methods<'_, '_, '_, 0>>() <= 15 * mem::size_of::<u64>());
};

impl From<MethodKey> for num::NonZero<u64> {
    fn from(value: MethodKey) -> Self {
        value.0.parts().into_raw()
    }
}

// SAFETY: Methods exclusively owns its order and pending storage; shared access cannot mutate either.
unsafe impl<const ID: u8> depot::raw::KeySource<Plane> for &methods::Methods<'_, '_, '_, ID> {
    type Key = MethodKey;

    fn fifo(&self) -> &keyed::Fifo<MethodKey, Plane> {
        &self.order
    }
}

// SAFETY: the original mutable root retains both the FIFO and its initialized storage.
unsafe impl<const ID: u8> depot::raw::KeySource<Plane> for &mut methods::Methods<'_, '_, '_, ID> {
    type Key = MethodKey;

    fn fifo(&self) -> &keyed::Fifo<MethodKey, Plane> {
        &self.order
    }
}
