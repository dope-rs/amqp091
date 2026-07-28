use std::pin::Pin;

use dope::driver::ready::{CompletionKey, CompletionSlot, CompletionWaker};
use dope_fiber::raw::task::Context;
use dope_fiber::raw::wait::{WaitQueue, Waiter};

/// Allocation-free task notification slot whose `'d` brand keeps the
/// notification inside the owning executor domain.
pub(crate) struct WakeSlot<'d>(CompletionSlot<'d>);

impl<'d> WakeSlot<'d> {
    pub(crate) const fn new() -> Self {
        Self(CompletionSlot::empty())
    }

    #[must_use]
    pub(crate) fn register(&self, wake: CompletionWaker<'d>) -> bool {
        if !self.0.is_empty() && !self.0.matches(&wake) {
            return false;
        }
        self.0.set(wake);
        true
    }

    pub(crate) fn wake(&self) {
        if let Some(wake) = self.0.take() {
            wake.wake();
        }
    }

    pub(crate) fn clear(&self) {
        self.0.clear();
    }

    pub(crate) fn clear_if(&self, key: CompletionKey<'d>) {
        self.0.clear_if(key);
    }
}

impl Default for WakeSlot<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixed-capacity intrusive registry for cold connection-wide events.
/// Registration, refresh, and cancellation are allocation-free and O(1).
pub(crate) struct WakeRegistry {
    queue: Pin<Box<WaitQueue>>,
}

impl WakeRegistry {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Box::pin(WaitQueue::with_capacity(capacity)),
        }
    }

    pub(crate) fn can_register<'d>(&self, waiter: Pin<&Waiter<'d>>) -> bool {
        self.queue.as_ref().can_register(waiter)
    }

    #[must_use]
    pub(crate) fn register<'d>(
        &self,
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        self.queue.as_ref().try_register(waiter, context)
    }

    pub(crate) fn wake(&self) {
        self.queue.as_ref().wake();
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::{WakeRegistry, WakeSlot};

    #[test]
    fn wake_owners_keep_compact_layouts() {
        assert_eq!(size_of::<WakeSlot<'static>>(), 3 * size_of::<usize>());
        assert_eq!(size_of::<WakeRegistry>(), size_of::<usize>());
    }
}
