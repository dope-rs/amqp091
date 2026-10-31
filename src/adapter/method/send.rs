use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue::Registration;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::channel::route;
use crate::adapter::method::admit::Admit;
use crate::adapter::method::wait;
use crate::adapter::{core, step};
use crate::failure;
use crate::protocol::method;
use crate::service::{self, cancel};
use crate::state::channel::cancel::lanes;
use crate::state::work::requests;
use crate::state::work::requests::Defer;

pub(in crate::adapter::method) enum Poll<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M> {
    Pending(Send<'o, 'c, 'd, ID, M>),
    Ready(wait::Wait<'o, 'r, 'c, 'b, 'd, ID>, wake::Waker<'d>),
    Error(failure::Runtime<'c, 'b, 'd, ID>, Send<'o, 'c, 'd, ID, M>),
}

pub(in crate::adapter::method) struct Send<'o, 'c: 'o, 'd: 'c, const ID: u8, M> {
    pub(in crate::adapter::method) ticket: cancel::Ticket<'o, 'd, lanes::Method, ID>,
    method: M,
    credit: Registration<'o, 'd, fifo::Scope<'c>>,
}

impl<'o, 'c: 'o, 'd: 'c, const ID: u8, M> Send<'o, 'c, 'd, ID, M> {
    pub(in crate::adapter::method) const fn new(
        ticket: cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: M,
    ) -> Self {
        Self {
            ticket,
            method,
            credit: Registration::new(),
        }
    }

    pub(in crate::adapter::method) fn poll<'r: 'o, 'b: 'c>(
        mut self,
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        mut access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> Poll<'o, 'r, 'c, 'b, 'd, ID, M>
    where
        M: method::Op<'d>,
    {
        let core = route.core();
        let admit = <<M as requests::Source<'d>>::Policy<ID> as Admit<'d, M, ID>>::admit(
            route,
            access.reborrow(),
            &self.ticket,
            &self.method,
        );
        let admit = match admit {
            Ok(admit) => admit,
            Err(Defer::Wait) => {
                self.credit.clear();
                let state = route.with(access, |route| {
                    route
                        .channel
                        .method_state()
                        .methods
                        .register(self.ticket.identity(), wake)
                });
                return match state {
                    service::EpochAccess::Live(Some(Ok(()))) => Poll::Pending(self),
                    service::EpochAccess::Live(Some(Err(error))) => {
                        Poll::Error(failure::Runtime::from(error), self)
                    }
                    service::EpochAccess::Terminal(cause) => Poll::Error(cause.branded(), self),
                    service::EpochAccess::Live(None) | service::EpochAccess::Unknown => {
                        Poll::Error(failure::Runtime::from(failure::Error::Closed), self)
                    }
                };
            }
        };
        match step::Step::admit(route, access, admit, &mut self.credit, wake) {
            step::Step::Pending => Poll::Pending(self),
            step::Step::Error(error) => Poll::Error(error, self),
            step::Step::Ready(index, next) => {
                core.permits().wake_channel(index);
                Poll::Ready(wait::Wait::new(route.receipt(), self.ticket), next)
            }
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Send<'static, 'static, 'static, 0, method::channel::Open>>()
        == 6 * std::mem::size_of::<usize>()
);
