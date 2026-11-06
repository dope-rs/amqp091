use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::conn::{Epoch, Ready};
use crate::adapter::{core, step};
use crate::failure;
use crate::protocol::method::conn::CloseRef;
use crate::protocol::method::{self};
use crate::protocol::wire::text::short;
use crate::service;

enum State<'text, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Send {
        gate: core::Gate<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
        reply_code: u16,
        reply_text: short::Short<'text>,
        credit: step::Wait<'o, 'c, 'd>,
    },
    Error(failure::Error),
    Done,
}

/// Admission state for one exact graceful connection close.
#[must_use = "a connection close must be polled or dropped"]
pub struct Start<'text, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    state: State<'text, 'o, 'r, 'c, 'b, 'd, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'text, 'd: 'b, const ID: u8> Start<'text, 'o, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::adapter::conn) fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
        ready: Ready<'d, ID>,
        code: u16,
        text: &'text str,
    ) -> Self {
        let state = match short::Short::new(text) {
            Ok(text) => State::Send {
                gate: core::Gate::new(core, ready),
                reply_code: code,
                reply_text: text,
                credit: step::Wait::new(),
            },
            Err(error) => State::Error(error),
        };
        Self { state }
    }

    pub fn poll(
        &mut self,
        access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        match self.step(access, wake) {
            step::Step::Pending => task::Poll::Pending,
            step::Step::Ready(_, _) => task::Poll::Ready(Ok(())),
            step::Step::Error(error) => task::Poll::Ready(Err(error)),
        }
    }

    pub(in crate::adapter::conn) fn step(
        &mut self,
        mut access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> step::Step<
        'c,
        'b,
        'd,
        (
            core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
            Epoch<'d, ID>,
        ),
        ID,
    > {
        let (gate, code, text, credit) = match &mut self.state {
            State::Send {
                gate,
                reply_code: code,
                reply_text: text,
                credit,
            } => (*gate, *code, *text, credit),
            State::Error(error) => {
                let error = mem::replace(error, failure::Error::Polled);
                self.state = State::Done;
                return step::Step::Error(failure::Runtime::from(error));
            }
            State::Done => {
                return step::Step::Error(failure::Runtime::from(failure::Error::Polled));
            }
        };
        let target = gate.epoch();
        let step = match gate.read(access.read(), |state| state.connection.input.is_ready()) {
            service::EpochAccess::Live(true) => {
                let admit = gate.admit(
                    access.reborrow(),
                    None,
                    |output| {
                        let close = CloseRef {
                            reply_code: code,
                            reply_text: text,
                            class_id: 0,
                            method_id: 0,
                        };
                        method::Encode::write(&close, output, 0);
                    },
                    |state| {
                        state.connection.start_close(target.raw())?;
                        Ok((gate.core(), target))
                    },
                );
                step::Step::admit(gate, access, admit, credit.credit(), wake)
            }
            service::EpochAccess::Live(false) => match credit.input(gate, access, &wake) {
                Ok(()) => step::Step::Pending,
                Err(error) => step::Step::Error(error),
            },
            service::EpochAccess::Terminal(cause) => step::Step::Error(cause.branded()),
            service::EpochAccess::Unknown => {
                step::Step::Error(failure::Runtime::from(failure::Error::Closed))
            }
        };
        match &step {
            step::Step::Pending => return step,
            step::Step::Ready(_, _) => gate.core().waiters().wake_flow(),
            step::Step::Error(_) => credit.cancel(gate.core().permits()),
        }
        self.state = State::Done;
        step
    }
}

impl<const ID: u8> Drop for Start<'_, '_, '_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        if let State::Send { gate, credit, .. } = &mut self.state {
            credit.cancel(gate.core().permits());
        }
    }
}

const _: () = assert!(
    mem::size_of::<Start<'static, 'static, 'static, 'static, 'static, 'static, 0>>()
        == 9 * mem::size_of::<usize>()
);
