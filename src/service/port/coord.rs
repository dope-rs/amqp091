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

    pub(crate) fn is_runnable(&self) -> bool {
        self.cancellations.has_pending() || self.cancellations.deferred_control.get()
    }

    pub(crate) fn drain<'a, 'turn>(
        &'a self,
        work: app::Work<'turn, 'd>,
        control: bool,
    ) -> quota::Admission<(cancel::Signal<'a, 'd, ID>, app::Permit<'turn, 'd>)> {
        let admit = work.claim(|| self.cancellations.pop(control));
        self.cancellations.deferred_control.set(
            matches!(admit, quota::Admission::Exhausted)
                && control
                && self.cancellations.has_control(),
        );
        admit
    }
}
