use std::task;

use dope::core::driver::schedule::ready::wake::queue;
use dope_amqp091::adapter::channel::Lease;
use dope_amqp091::adapter::conn;
use dope_amqp091::protocol::method::channel;
use dope_amqp091::{adapter, failure};
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use dope_test::harness::support::gates::Barrier;
use o3::base::cell::local;
use o3::depot::queue::resident::fifo;

use crate::common::wake::Fault;

pub struct Flow<'g, 'o, 'r, 'c, 'b, 'd> {
    pub(crate) adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) target: conn::Epoch<'d, 0>,
    pub(crate) lease: Lease<'c, 'd, 0>,
    pub(crate) credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
    pub(crate) gate: Option<&'g Barrier>,
}

impl<'o, 'r, 'c, 'b, 'd> Fiber<'d> for Flow<'_, 'o, 'r, 'c, 'b, 'd> {
    type Output = Result<(), Fault<failure::Runtime<'c, 'b, 'd, 0>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let wake = cx.as_ref().completion();
        let local = local::Access::new(cx.as_mut().region_token());
        let poll = this.adapter.channels().poll_send(
            local,
            this.target,
            this.lease,
            &channel::Flow { active: true },
            &mut this.credit,
            wake,
        );
        if poll.is_pending()
            && let Some(gate) = this.gate.take()
        {
            gate.arrive().map_err(Fault::Notify)?;
        }
        poll.map_err(Fault::Runtime)
    }
}
