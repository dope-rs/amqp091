use std::task;

use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter::{channel, conn};
use dope_amqp091::failure;
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use o3::base::cell::local;

use crate::common::wake::Fault;

pub enum Phase {
    Wait,
    Retire,
    Done,
}

pub struct Progress<'o, 'r, 'c, 'b, 'd> {
    pub(crate) channels: channel::Channels<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) connection: conn::Connection<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) target: conn::Epoch<'d, 0>,
    pub(crate) wait: fanout::Registration<'o, 'd>,
    pub(crate) phase: Phase,
}

impl<'o, 'r, 'c, 'b, 'd> Fiber<'d> for Progress<'o, 'r, 'c, 'b, 'd> {
    type Output = Result<(), Fault<failure::Error>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        if matches!(this.phase, Phase::Done) {
            return task::Poll::Ready(Ok(()));
        }
        let wake = cx.as_ref().completion();
        this.channels
            .watch_flow(&mut this.wait, &wake)
            .map_err(Fault::Runtime)?;
        if matches!(this.phase, Phase::Retire) {
            let local = local::Access::new(cx.as_mut().region_token());
            this.connection.control().retire(local, this.target);
        }
        this.phase = Phase::Done;
        task::Poll::Pending
    }
}
