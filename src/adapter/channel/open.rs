use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::{Lease, Reservation};
use crate::adapter::{self, method};
use crate::failure;
use crate::protocol::method::channel;

enum State<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Call {
        adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
        reservation: Reservation<'o, 'c, 'd, ID>,
        call: method::Call<'o, 'r, 'c, 'b, 'd, ID, channel::Open>,
    },
    Done,
}

/// An exact channel reservation joined to its protocol open call.
#[must_use = "a channel open must be polled or cancelled"]
pub struct Open<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    state: State<'o, 'r, 'c, 'b, 'd, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Open<'o, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::adapter) const fn new(
        amqp: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
        slot: Reservation<'o, 'c, 'd, ID>,
        call: method::Call<'o, 'r, 'c, 'b, 'd, ID, channel::Open>,
    ) -> Self {
        Self {
            state: State::Call {
                adapter: amqp,
                reservation: slot,
                call,
            },
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> adapter::Poll<'c, 'b, 'd, ID>
    for Open<'o, 'r, 'c, 'b, 'd, ID>
{
    type Output = Lease<'c, 'd, ID>;

    fn poll(
        &mut self,
        mut access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Lease<'c, 'd, ID>, failure::Runtime<'c, 'b, 'd, ID>>> {
        let state = mem::replace(&mut self.state, State::Done);
        let State::Call {
            adapter: amqp,
            reservation: slot,
            mut call,
        } = state
        else {
            return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
        };
        match call.poll(access.reborrow(), wake) {
            task::Poll::Pending => {
                self.state = State::Call {
                    adapter: amqp,
                    reservation: slot,
                    call,
                };
                task::Poll::Pending
            }
            task::Poll::Ready(Ok(())) => task::Poll::Ready(Ok(slot.commit())),
            task::Poll::Ready(Err(error)) => {
                amqp.channels().release(access, slot);
                task::Poll::Ready(Err(error))
            }
        }
    }
}

const _: () = assert!(
    mem::size_of::<Open<'static, 'static, 'static, 'static, 'static, 0>>()
        == 16 * mem::size_of::<usize>()
);
