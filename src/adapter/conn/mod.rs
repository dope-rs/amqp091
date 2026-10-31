use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::fanout;
use dope::manifold::client::close::LocalOrigin;
use o3::base::cell::{local, region};

use crate::adapter::core;
use crate::failure::terminal;
use crate::state::conn::Change;
use crate::state::conn::status::Status;
use crate::state::epoch::Id;
use crate::{failure, protocol, service};

mod close;
mod epoch;
mod ready;
mod start;
mod view;

pub use close::Close;
pub use epoch::Epoch;
pub use ready::Ready;
pub use start::Start;
pub use view::View;

/// One-word connection capability root. It has projection authority only.
#[repr(transparent)]
pub struct Connection<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Conn>,
}

impl<const ID: u8> Copy for Connection<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Connection<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Connection<'o, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Conn>) -> Self {
        Self { core }
    }

    pub const fn observe(self) -> Observe<'o, 'r, 'c, 'b, 'd, ID> {
        Observe {
            core: self.core.project(),
        }
    }

    pub const fn control(self) -> Control<'o, 'r, 'c, 'b, 'd, ID> {
        Control {
            core: self.core.project(),
        }
    }
}

/// Read-only connection authority.
#[repr(transparent)]
pub struct Observe<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(in crate::adapter) core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Observe>,
}

impl<const ID: u8> Copy for Observe<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Observe<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Observe<'o, 'r, 'c, 'b, 'd, ID> {
    pub fn read<R>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        f: impl for<'state> FnOnce(View<'state, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> R {
        self.core.shared().read(access, |state| f(View { state }))
    }

    pub fn read_in<R>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        ready: Ready<'d, ID>,
        f: impl for<'state> FnOnce(View<'state, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Result<R, failure::Runtime<'c, 'b, 'd, ID>> {
        match core::Gate::new(self.core, ready).read(access, |state| f(View { state })) {
            service::EpochAccess::Live(output) => Ok(output),
            service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            service::EpochAccess::Unknown => Err(failure::Runtime::from(failure::Error::Closed)),
        }
    }

    pub fn is_active(self, epoch: Epoch<'d, ID>) -> bool {
        self.core.is_active(epoch.raw())
    }

    pub fn negotiated(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        epoch: Epoch<'d, ID>,
    ) -> Result<protocol::Negotiated, failure::Runtime<'c, 'b, 'd, ID>> {
        let active = self.core.is_active(epoch.raw());
        self.core.shared().read(access, |state| {
            if active {
                Ok(state.connection.negotiated())
            } else {
                Err(state
                    .connection
                    .lifecycle
                    .status()
                    .terminal(epoch.raw())
                    .map(terminal::Outcome::retain)
                    .map_or(
                        failure::Runtime::from(failure::Error::Closed),
                        terminal::Outcome::branded,
                    ))
            }
        })
    }

    pub fn poll_retired(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: Epoch<'d, ID>,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        let live = self.core.shared().read(access.read(), |state| {
            state.connection.lifecycle.status().source() == Some(target.raw())
        });
        if !live || !self.core.is_active(target.raw()) {
            wait.clear();
            return task::Poll::Ready(Ok(()));
        }
        if let Err(error) = self.core.waiters().watch_state(wait, &wake) {
            return task::Poll::Ready(Err(failure::Runtime::from(error)));
        }
        task::Poll::Pending
    }

    pub fn poll_unblock(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        let block = self.core.shared().read(access.read(), |state| {
            let status = state.connection.lifecycle.status();
            if let Some(error) = status.rejection() {
                Err(failure::Runtime::from(error))
            } else if let Some(cause) = status.failure() {
                Err(terminal::Outcome::retain(cause).branded())
            } else {
                Ok(state.connection.blocked().is_some())
            }
        });
        match block {
            Err(error) => {
                wait.clear();
                task::Poll::Ready(Err(error))
            }
            Ok(false) => {
                wait.clear();
                task::Poll::Ready(Ok(()))
            }
            Ok(true) => match self.core.waiters().watch_flow(wait, &wake) {
                Ok(()) => task::Poll::Pending,
                Err(error) => task::Poll::Ready(Err(failure::Runtime::from(error))),
            },
        }
    }

    pub fn poll_epoch(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Epoch<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>> {
        self.poll_state(access, wait, wake, Status::epoch, Epoch::from_raw)
    }

    pub fn poll_ready(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Ready<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>> {
        self.poll_state(access, wait, wake, Status::ready, Ready::from_raw)
    }

    fn poll_state<T>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
        gate: impl Copy + Fn(&Status<'c, 'b, 'd, ID>) -> Option<Id<'d, ID>>,
        proof: impl FnOnce(Id<'d, ID>) -> T,
    ) -> task::Poll<Result<T, failure::Runtime<'c, 'b, 'd, ID>>> {
        let state = self.core.shared().read(access.read(), |state| {
            let status = state.connection.lifecycle.status();
            if let Some(error) = status.rejection() {
                return Err(failure::Runtime::from(error));
            }
            match status {
                Status::Closing(_, cause, _) => Err(terminal::Outcome::retain(cause).branded()),
                Status::Closed(_, cause, _, _) | Status::Failed(_, cause) => {
                    Err(terminal::Outcome::retain(cause).branded())
                }
                _ => Ok((status.epoch(), gate(status))),
            }
        });
        let (epoch, target) = match state {
            Ok(state) => state,
            Err(error) => {
                wait.clear();
                return task::Poll::Ready(Err(error));
            }
        };
        if let Some(epoch) = epoch {
            if self.core.is_active(epoch) {
                if let Some(target) = target {
                    wait.clear();
                    return task::Poll::Ready(Ok(proof(target)));
                }
            } else {
                match self
                    .core
                    .shared()
                    .write(access, |state| state.retire_epoch(epoch))
                {
                    Change::Applied => self.core.waiters().wake_all(),
                    Change::Ignored => {}
                }
            }
        }
        match self.core.waiters().watch_state(wait, &wake) {
            Ok(()) => task::Poll::Pending,
            Err(error) => task::Poll::Ready(Err(failure::Runtime::from(error))),
        }
    }
}

/// Connection command admission authority.
#[repr(transparent)]
pub struct Control<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
}

impl<const ID: u8> Copy for Control<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Control<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Control<'o, 'r, 'c, 'b, 'd, ID> {
    pub fn retire(self, access: local::Access<'_, region::Identity<'d>>, epoch: Epoch<'d, ID>) {
        self.core.shared().write(access, |state| {
            state.retire(epoch.raw(), failure::Reason::Local(LocalOrigin::ApiRequest));
        });
        self.core.waiters().wake_all();
    }

    pub fn start<'text>(
        self,
        ready: Ready<'d, ID>,
        code: u16,
        text: &'text str,
    ) -> Start<'text, 'o, 'r, 'c, 'b, 'd, ID> {
        Start::new(self.core, ready, code, text)
    }

    pub fn close<'text>(
        self,
        ready: Ready<'d, ID>,
        code: u16,
        text: &'text str,
    ) -> Close<'text, 'o, 'r, 'c, 'b, 'd, ID> {
        Close::new(self.core, ready, code, text)
    }
}

const _: () = assert!(
    mem::size_of::<Connection<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Observe<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Control<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
