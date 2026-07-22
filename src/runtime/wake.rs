use std::cell::Cell;

use dope_fiber::Waker;

/// Allocation-free task notification slot whose `'d` brand keeps the
/// notification inside the owning executor domain.
pub(crate) struct WakeSlot<'d>(Cell<Option<Waker<'d>>>);

impl<'d> WakeSlot<'d> {
    pub(crate) const fn new() -> Self {
        Self(Cell::new(None))
    }

    #[must_use]
    pub(crate) fn register(&self, waker: Waker<'d>) -> bool {
        if self.0.get().is_some_and(|current| current != waker) {
            return false;
        }
        self.0.set(Some(waker));
        true
    }

    pub(crate) fn wake(&self) {
        if let Some(waker) = self.0.take() {
            waker.wake();
        }
    }

    pub(crate) fn clear(&self) {
        self.0.set(None);
    }
}

impl Default for WakeSlot<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixed-capacity registry for cold connection-wide events. Its compact linear
/// storage keeps registration and wake allocation-free and idempotent.
pub(crate) struct WakeRegistry<'d> {
    slots: Box<[Cell<Option<Waker<'d>>>]>,
}

impl<'d> WakeRegistry<'d> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| Cell::new(None)).collect(),
        }
    }

    pub(crate) fn can_register(&self, waker: Waker<'d>) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.get().is_none_or(|current| current == waker))
    }

    #[must_use]
    pub(crate) fn register(&self, waker: Waker<'d>) -> bool {
        let mut vacant = None;
        for slot in &self.slots {
            match slot.get() {
                Some(current) if current == waker => return true,
                None if vacant.is_none() => vacant = Some(slot),
                Some(_) | None => {}
            }
        }
        let Some(slot) = vacant else {
            return false;
        };
        slot.set(Some(waker));
        true
    }

    pub(crate) fn wake(&self) {
        for slot in &self.slots {
            if let Some(waker) = slot.take() {
                waker.wake();
            }
        }
    }
}
