use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::fanout;
use o3::base::cell::{local, region};

use crate::adapter::conn::{Epoch, Ready, Start};
use crate::adapter::{core, step};
use crate::failure;
use crate::failure::terminal;
use crate::state::conn::status::CloseResult;

struct Wait<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
    target: Epoch<'d, ID>,
    registration: fanout::Registration<'o, 'd>,
}

enum State<'text, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Start(Start<'text, 'o, 'r, 'c, 'b, 'd, ID>),
    Wait(Wait<'o, 'r, 'c, 'b, 'd, ID>),
    Done,
}

/// An exact graceful close from admission through `connection.close-ok`.
#[must_use = "a connection close must be polled or dropped"]
pub struct Close<'text, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    state: State<'text, 'o, 'r, 'c, 'b, 'd, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'text, 'd: 'b, const ID: u8> Close<'text, 'o, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::adapter::conn) fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
        ready: Ready<'d, ID>,
        code: u16,
        text: &'text str,
    ) -> Self {
        Self {
            state: State::Start(Start::new(core, ready, code, text)),
        }
    }

    pub fn poll(
        &mut self,
        mut access: local::Access<'_, region::Identity<'d>>,
        mut wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        loop {
            let state = mem::replace(&mut self.state, State::Done);
            match state {
                State::Start(mut start) => match start.step(access.reborrow(), wake) {
                    step::Step::Pending => {
                        self.state = State::Start(start);
                        return task::Poll::Pending;
                    }
                    step::Step::Ready((core, target), next) => {
                        self.state = State::Wait(Wait::new(core, target));
                        wake = next;
                    }
                    step::Step::Error(error) => return task::Poll::Ready(Err(error)),
                },
                State::Wait(mut wait) => match wait.poll(access.reborrow(), wake) {
                    task::Poll::Pending => {
                        self.state = State::Wait(wait);
                        return task::Poll::Pending;
                    }
                    task::Poll::Ready(output) => return task::Poll::Ready(output),
                },
                State::Done => {
                    return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
                }
            }
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Wait<'o, 'r, 'c, 'b, 'd, ID> {
    const fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
        target: Epoch<'d, ID>,
    ) -> Self {
        Self {
            core,
            target,
            registration: fanout::Registration::new(),
        }
    }

    fn poll(
        &mut self,
        access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        self.core.shared().read(access.read(), |state| {
            match state
                .connection
                .lifecycle
                .status()
                .close_result(self.target.raw())
            {
                CloseResult::Pending => {}
                CloseResult::Complete => {
                    self.registration.clear();
                    return task::Poll::Ready(Ok(()));
                }
                CloseResult::Terminal(cause) => {
                    self.registration.clear();
                    return task::Poll::Ready(Err(terminal::Outcome::retain(cause).branded()));
                }
                CloseResult::Unknown => {
                    self.registration.clear();
                    return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Closed)));
                }
            }
            match self
                .core
                .waiters()
                .watch_close(&mut self.registration, &wake)
            {
                Ok(()) => task::Poll::Pending,
                Err(error) => task::Poll::Ready(Err(failure::Runtime::from(error))),
            }
        })
    }
}

const _: () = assert!(
    mem::size_of::<Close<'static, 'static, 'static, 'static, 'static, 'static, 0>>()
        == 8 * mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Wait<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<core::Core<'static, 'static, 'static, 'static, 'static, 0, core::Command>>(
        ) + mem::size_of::<Epoch<'static, 0>>()
            + mem::size_of::<fanout::Registration<'static, 'static>>()
);
