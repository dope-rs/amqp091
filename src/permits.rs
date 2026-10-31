use std::num;

use dope::core::driver::schedule::app;
use dope::core::driver::schedule::ready::wake::{self, queue};
use dope::core::driver::{self};
use o3::depot::queue::resident::{self, fifo};

use crate::failure::{self, wire};

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    capacity: usize,
    lanes: num::NonZero<usize>,
}

impl Plan {
    pub(crate) fn try_new(capacity: usize, channels: usize) -> Result<Self, failure::Error> {
        let Some(lanes) = num::NonZero::<usize>::MIN.checked_add(channels) else {
            return Err(failure::Error::InvalidConfig(
                wire::Config::AdmissionLanes { channels },
            ));
        };
        Ok(Self { capacity, lanes })
    }
}

/// AMQP's semantic lane mapping over the driver-owned queued completion
/// registry: lane zero is transport credit and every remaining lane is a
/// physical channel slot.
#[repr(transparent)]
pub(crate) struct Registry<'scope, 'd: 'scope>(queue::Queue<'d, fifo::Scope<'scope>>);

impl<'d> Registry<'_, 'd> {
    pub(crate) fn try_with<S, R>(
        driver: driver::Reference<'d>,
        plan: Plan,
        source: S,
        run: impl for<'scope> FnOnce(&'scope S, Registry<'scope, 'd>) -> R,
    ) -> Result<R, resident::Error> {
        queue::Queue::try_with(
            driver,
            plan.capacity,
            plan.lanes,
            (driver, source),
            |(_, source), queue| run(source, Registry(queue)),
        )
    }
}

impl<'scope, 'd: 'scope> Registry<'scope, 'd> {
    pub(crate) fn try_index(
        &self,
        position: usize,
    ) -> Result<fifo::Index<'scope>, resident::Bounds<num::NonZero<u32>>> {
        self.0.try_index(position)
    }

    pub(crate) fn wait_credit<'target>(
        &'target self,
        slot: &mut queue::Registration<'target, 'd, fifo::Scope<'scope>>,
        target: &wake::Waker<'d>,
    ) -> Result<(), failure::Error> {
        self.0
            .try_first(slot, target)
            .map_err(queue::Error::from)
            .map_err(failure::Error::Queue)
    }

    pub(crate) fn wait_channel<'target>(
        &'target self,
        lane: fifo::Index<'scope>,
        slot: &mut queue::Registration<'target, 'd, fifo::Scope<'scope>>,
        target: &wake::Waker<'d>,
    ) -> Result<(), failure::Error> {
        self.0
            .try_register(lane, slot, target)
            .map_err(queue::Error::from)
            .map_err(failure::Error::Queue)
    }

    pub(crate) fn wake_credit(&self) -> wake::Once {
        self.0.wake_first()
    }

    pub(crate) fn wake_channel(&self, lane: fifo::Index<'scope>) {
        self.0.wake_one(lane);
    }

    pub(crate) fn wake_all(&self) {
        self.0.begin_drain();
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.0.is_draining()
    }

    pub(crate) fn flush(&self, work: app::Work<'_, 'd>) {
        loop {
            match self.0.drain_one(work) {
                wake::Drain::Woke => {}
                wake::Drain::Empty | wake::Drain::Exhausted => {
                    return;
                }
            }
        }
    }
}
