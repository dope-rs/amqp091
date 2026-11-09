use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::{Lease, Reservation};
use crate::adapter::{self, method};
use crate::failure;
use crate::protocol::method::channel;

/// An exact channel reservation joined to its protocol open call.
#[must_use = "a channel open must be polled or cancelled"]
pub struct Open<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
    reservation: Option<Reservation<'o, 'c, 'd, ID>>,
    call: method::Call<'o, 'r, 'c, 'b, 'd, ID, channel::Open>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Open<'o, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::adapter) const fn new(
        amqp: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
        slot: Reservation<'o, 'c, 'd, ID>,
        call: method::Call<'o, 'r, 'c, 'b, 'd, ID, channel::Open>,
    ) -> Self {
        Self {
            adapter: amqp,
            reservation: Some(slot),
            call,
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
        let result = match self.call.poll(access.reborrow(), wake) {
            task::Poll::Pending => return task::Poll::Pending,
            task::Poll::Ready(result) => result,
        };
        let Some(slot) = self.reservation.take() else {
            return task::Poll::Ready(result.map(|()| unreachable!("open")));
        };
        task::Poll::Ready(match result {
            Ok(()) => Ok(slot.commit()),
            Err(error) => {
                self.adapter.channels().release(access, slot);
                Err(error)
            }
        })
    }
}

const _: () = assert!(
    mem::size_of::<Open<'static, 'static, 'static, 'static, 'static, 0>>()
        == 16 * mem::size_of::<usize>()
);
