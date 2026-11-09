use crate::service::{Waiters, cancel};

pub(crate) struct Coord<'d, const ID: u8> {
    pub(in crate::service) waiters: Waiters<'d>,
    pub(crate) cancellations: cancel::Registry<'d, ID>,
}

impl<'d, const ID: u8> Coord<'d, ID> {
    pub(crate) fn waiters(&self) -> &Waiters<'d> {
        &self.waiters
    }
}
