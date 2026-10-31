use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::conn::{Epoch, Ready};
use crate::adapter::{core, step};
use crate::failure;
use crate::protocol::method::conn::CloseRef;
use crate::protocol::method::{self};
use crate::protocol::wire::text::short;

enum State<'text, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Send {
        gate: core::Gate<'o, 'r, 'c, 'b, 'd, ID, core::Command>,
        reply_code: u16,
        reply_text: short::Short<'text>,
        credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
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
                credit: queue::Registration::new(),
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
        let state = mem::replace(&mut self.state, State::Done);
        let (gate, code, text, mut credit) = match state {
            State::Send {
                gate,
                reply_code: code,
                reply_text: text,
                credit,
            } => (gate, code, text, credit),
            State::Error(error) => {
                return step::Step::Error(failure::Runtime::from(error));
            }
            State::Done => {
                return step::Step::Error(failure::Runtime::from(failure::Error::Polled));
            }
        };
        let target = gate.epoch();
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
        match step::Step::admit(gate, access, admit, &mut credit, wake) {
            step::Step::Pending => {
                self.state = State::Send {
                    gate,
                    reply_code: code,
                    reply_text: text,
                    credit,
                };
                step::Step::Pending
            }
            step::Step::Ready(wait, done) => step::Step::Ready(wait, done),
            step::Step::Error(error) => step::Step::Error(error),
        }
    }
}

const _: () = assert!(
    mem::size_of::<Start<'static, 'static, 'static, 'static, 'static, 'static, 0>>()
        == 8 * mem::size_of::<usize>()
);
