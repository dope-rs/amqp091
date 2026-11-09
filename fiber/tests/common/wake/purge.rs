use std::task;

use dope_amqp091::adapter::{Poll as _, method};
use dope_amqp091::failure;
use dope_amqp091::protocol::method::queue;
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use dope_test::alloc::Tracker;
use o3::base::cell::local;

use crate::common::wake::Fault;

pub struct Purge<'a, 'name, 'o, 'r, 'c, 'b, 'd> {
    pub(crate) call: &'a mut method::Call<'o, 'r, 'c, 'b, 'd, 0, queue::Purge<'name>>,
}

impl<'name, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b> Fiber<'d> for Purge<'_, 'name, 'o, 'r, 'c, 'b, 'd> {
    type Output = Result<queue::PurgeOk, Fault<failure::Runtime<'c, 'b, 'd, 0>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let wake = cx.as_ref().completion();
        let local = local::Access::new(cx.as_mut().region_token());
        let (poll, alloc) = Tracker::usage(|| this.call.poll(local, wake));
        if alloc != (0, 0) {
            return task::Poll::Ready(Err(Fault::Allocations {
                count: alloc.0,
                bytes: alloc.1,
            }));
        }
        poll.map_err(Fault::Runtime)
    }
}
