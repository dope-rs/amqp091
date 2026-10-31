use std::cell;

use dope::core::driver::schedule::app;
use dope::core::driver::schedule::ready::wake::{self, fanout};
use dope::core::driver::{self};
use o3::depot::queue::resident;

use crate::failure;

pub(crate) struct Waiters<'d> {
    state: fanout::Set<'d>,
    closed: fanout::Set<'d>,
    progress: fanout::Set<'d>,
    drain: cell::Cell<u8>,
}

impl<'d> Waiters<'d> {
    pub(crate) fn try_new(
        driver: driver::Reference<'d>,
        limit: usize,
    ) -> Result<Self, resident::Error> {
        Ok(Self {
            state: fanout::Set::try_new(driver, limit)?,
            closed: fanout::Set::try_new(driver, limit)?,
            progress: fanout::Set::try_new(driver, limit)?,
            drain: cell::Cell::new(0),
        })
    }

    pub(crate) fn watch_state<'a>(
        &'a self,
        slot: &mut fanout::Registration<'a, 'd>,
        target: &wake::Waker<'d>,
    ) -> Result<(), failure::Error> {
        self.state
            .try_register(slot, target)
            .map_err(failure::Error::Fanout)
    }

    pub(crate) fn watch_close<'a>(
        &'a self,
        slot: &mut fanout::Registration<'a, 'd>,
        target: &wake::Waker<'d>,
    ) -> Result<(), failure::Error> {
        self.closed
            .try_register(slot, target)
            .map_err(failure::Error::Fanout)
    }

    pub(crate) fn watch_flow<'a>(
        &'a self,
        slot: &mut fanout::Registration<'a, 'd>,
        target: &wake::Waker<'d>,
    ) -> Result<(), failure::Error> {
        self.progress
            .try_register(slot, target)
            .map_err(failure::Error::Fanout)
    }

    pub(crate) fn wake_state(&self) {
        self.state.publish();
    }

    pub(crate) fn wake_closed(&self) {
        self.closed.publish();
    }

    pub(crate) fn wake_flow(&self) {
        self.progress.publish();
    }

    pub(crate) fn wake_all(&self) {
        self.state.publish();
        self.closed.publish();
        self.progress.publish();
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.state.has_pending() || self.closed.has_pending() || self.progress.has_pending()
    }

    pub(crate) fn flush(&self, work: app::Work<'_, 'd>) {
        let mut empty = 0;
        while empty < 3 {
            let index = self.drain.get();
            self.drain.set((index + 1) % 3);
            let set = match index {
                0 => &self.state,
                1 => &self.closed,
                _ => &self.progress,
            };
            match set.drain_one(work) {
                wake::Drain::Woke => empty = 0,
                wake::Drain::Empty => empty += 1,
                wake::Drain::Exhausted => break,
            }
        }
    }
}
