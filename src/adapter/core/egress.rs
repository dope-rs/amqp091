use std::{ops, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::{channel, core};
use crate::failure::terminal;
use crate::protocol::wire::Writer;
use crate::state::epoch;
use crate::{failure, service};

enum Deferred<'c> {
    Blocked(fifo::Index<'c>),
    Closed,
}

#[repr(transparent)]
pub(in crate::adapter) struct Egress<
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const ID: u8,
    K: core::Capability,
> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: core::Capability>
    Egress<'o, 'r, 'c, 'b, 'd, ID, K>
{
    pub(in crate::adapter) const fn new(core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>) -> Self {
        Self { core }
    }

    pub(in crate::adapter) fn poll(
        self,
        mut cx: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        encode: impl FnOnce(&mut Writer),
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>>
    where
        K: core::Allows<core::State> + core::Allows<core::Egress> + core::Allows<core::Registry>,
    {
        let admit = self.core.outbound().admit_if(
            cx.reborrow(),
            target,
            Some(lease.raw()),
            encode,
            |state| match state.routes.project(lease.raw(), |route| {
                let state = route.channel.publish();
                let wait = state.is_waiting();
                let active = state.publishing.is_active();
                if route.channel.is_retiring() {
                    ops::ControlFlow::Break(Deferred::Closed)
                } else if active || wait {
                    ops::ControlFlow::Break(Deferred::Blocked(route.index.lane()))
                } else {
                    ops::ControlFlow::Continue(route.index.lane())
                }
            }) {
                Some(flow) => flow,
                None => ops::ControlFlow::Break(Deferred::Closed),
            },
        );
        match admit {
            Err(Deferred::Blocked(index)) => {
                match self.core.permits().wait_channel(index, credit, &wake) {
                    Ok(()) => task::Poll::Pending,
                    Err(error) => task::Poll::Ready(Err(failure::Runtime::from(error))),
                }
            }
            Err(Deferred::Closed) => {
                task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Closed)))
            }
            Ok(service::Admit::Item(index)) => {
                credit.clear();
                self.core.permits().wake_channel(index);
                task::Poll::Ready(Ok(()))
            }
            Ok(state @ (service::Admit::Unavailable | service::Admit::Inactive)) => {
                if matches!(state, service::Admit::Unavailable) {
                    let active = self.core.is_active(target);
                    let live = self.core.shared().write(cx.reborrow(), |state| {
                        active
                            && state.connection.lifecycle.status().epoch() == Some(target)
                            && state
                                .routes
                                .project(lease.raw(), |route| !route.channel.is_retiring())
                                == Some(true)
                    });
                    if live {
                        return match self.core.permits().wait_credit(credit, &wake) {
                            Ok(()) => task::Poll::Pending,
                            Err(error) => task::Poll::Ready(Err(failure::Runtime::from(error))),
                        };
                    }
                    credit.clear();
                }
                task::Poll::Ready(Err(self.core.shared().read(cx.read(), |state| {
                    state
                        .connection
                        .lifecycle
                        .status()
                        .terminal(target)
                        .map(terminal::Outcome::retain)
                        .map_or(
                            failure::Runtime::from(failure::Error::Closed),
                            terminal::Outcome::branded,
                        )
                })))
            }
            Ok(service::Admit::Error(error)) => {
                task::Poll::Ready(Err(failure::Runtime::from(error)))
            }
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Egress<'static, 'static, 'static, 'static, 'static, 0, core::Channels>>()
        == std::mem::size_of::<usize>()
);
