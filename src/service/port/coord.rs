use dope::core::driver::schedule::app;
use o3::mem::quota;

use crate::service::{Waiters, cancel};

pub(crate) struct Coord<'d, const ID: u8> {
    pub(in crate::service) waiters: Waiters<'d>,
    pub(crate) cancellations: cancel::Registry<'d, ID>,
}

impl<'d, const ID: u8> Coord<'d, ID> {
    pub(crate) fn waiters(&self) -> &Waiters<'d> {
        &self.waiters
    }

    pub(crate) fn drain<'turn>(
        &self,
        work: app::Work<'turn, 'd>,
    ) -> quota::Admission<(cancel::Signal<'d, ID>, app::Permit<'turn, 'd>)> {
        let admit = work.claim(|| self.cancellations.pop());
        if matches!(admit, quota::Admission::Exhausted) && self.cancellations.has_pending() {
            self.waiters.wake_flow();
        }
        admit
    }
}
