use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::receipt;
use crate::adapter::{consumer, core};
use crate::failure;
use crate::failure::terminal;
use crate::protocol::method;
use crate::service::{self, cancel};
use crate::state::channel::cancel::lanes;
use crate::state::channel::methods;

pub(in crate::adapter::method) enum Poll<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, T> {
    Pending(Wait<'o, 'r, 'c, 'b, 'd, ID>),
    Ready(T),
}

pub(in crate::adapter::method) struct Wait<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(in crate::adapter::method) receipt: receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
    pub(in crate::adapter::method) ticket: cancel::Ticket<'o, 'd, lanes::Method, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Wait<'o, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::adapter::method) const fn new(
        entry: receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        ticket: cancel::Ticket<'o, 'd, lanes::Method, ID>,
    ) -> Self {
        Self {
            receipt: entry,
            ticket,
        }
    }

    pub(in crate::adapter::method) fn poll<M>(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> Poll<
        'o,
        'r,
        'c,
        'b,
        'd,
        ID,
        Result<
            <M as method::Op<'d>>::Output<'b, consumer::Key<'o, 'd, ID>>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    >
    where
        M: method::Op<'d>,
    {
        let core = self.receipt.core();
        let state = self.receipt.with(access.reborrow(), |view| {
            Some(
                view.channel
                    .method_state()
                    .methods
                    .poll(self.ticket.identity(), wake)
                    .map(|state| state.map(|reply| (view.index, reply))),
            )
        });
        let out = match state {
            service::EpochAccess::Live(Ok(None)) => return Poll::Pending(self),
            service::EpochAccess::Live(Err(error)) => Err(failure::Runtime::from(error)),
            service::EpochAccess::Live(Ok(Some((index, result)))) => result
                .map_err(terminal::Outcome::branded)
                .and_then(|reply| {
                    let reply = reply
                        .try_map(|raw| {
                            match core.cancel().issue(
                                index,
                                self.receipt.lease().raw(),
                                lanes::Slot::from(raw),
                                raw,
                            ) {
                                Ok(ticket) => Ok(consumer::Key::from_cancel(ticket)),
                                Err(error) => {
                                    core.detach(
                                        access.reborrow(),
                                        self.receipt.lease(),
                                        methods::DetachAction::RetireConsumer(raw),
                                    );
                                    Err(error)
                                }
                            }
                        })
                        .map_err(failure::Runtime::from)?;
                    let body = <M::Response as method::Reply>::project(reply)
                        .map_err(failure::Runtime::from)?;
                    M::finish(body).map_err(failure::Runtime::from)
                }),
            service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            service::EpochAccess::Unknown => Err(failure::Runtime::from(failure::Error::Closed)),
        };
        self.ticket.complete();
        self.receipt.collect(access);
        Poll::Ready(out)
    }
}

const _: () = assert!(
    std::mem::size_of::<Wait<'static, 'static, 'static, 'static, 'static, 0>>()
        == 8 * std::mem::size_of::<usize>()
);
