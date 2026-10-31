use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::receipt;
use crate::adapter::core;
use crate::service::cancel;
use crate::state::channel::cancel::lanes;
use crate::{failure, protocol, service};

pub(super) enum Poll<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Pending(Wait<'o, 'r, 'c, 'b, 'd, ID>),
    Ready(Result<protocol::Confirmation, failure::Runtime<'c, 'b, 'd, ID>>),
}

pub(super) struct Wait<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(super) receipt: receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
    pub(super) ticket: cancel::Ticket<'o, 'd, lanes::Confirm, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Wait<'o, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(
        entry: receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        ticket: cancel::Ticket<'o, 'd, lanes::Confirm, ID>,
    ) -> Self {
        Self {
            receipt: entry,
            ticket,
        }
    }

    pub(super) fn poll(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> Poll<'o, 'r, 'c, 'b, 'd, ID> {
        let result = self.receipt.with(access.reborrow(), |view| {
            view.channel.confirm().poll(self.ticket.identity(), wake)
        });
        let out = match result {
            service::EpochAccess::Live(Ok(Some(item))) => {
                self.ticket.complete();
                self.receipt.collect(access);
                return Poll::Ready(Ok(item));
            }
            service::EpochAccess::Live(Ok(None)) => return Poll::Pending(self),
            service::EpochAccess::Live(Err(error)) => Err(error.branded()),
            service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            service::EpochAccess::Unknown => Err(failure::Runtime::Local(failure::Error::Closed)),
        };
        self.ticket.complete();
        Poll::Ready(out)
    }
}

const _: () = {
    use std::mem;

    assert!(
        mem::size_of::<Wait<'static, 'static, 'static, 'static, 'static, 0>>()
            == 8 * mem::size_of::<usize>()
    );
};
