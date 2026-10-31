use std::task;

use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter::{channel, conn};
use dope_amqp091::failure;
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use o3::base::cell::local;

#[derive(Debug)]
pub enum Error {
    Owner(failure::Error),
    Reused,
    Contender(failure::Error),
    Held(failure::Error),
}

pub enum Phase {
    Owner,
    Contender,
    Done,
}

pub struct Probe<'o, 'r, 'c, 'b, 'd> {
    pub(crate) channels: channel::Channels<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) connection: conn::Connection<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) target: conn::Epoch<'d, 0>,
    pub(crate) owner: fanout::Registration<'o, 'd>,
    pub(crate) contender: fanout::Registration<'o, 'd>,
    pub(crate) phase: Phase,
}

impl<'o, 'r, 'c, 'b, 'd> Fiber<'d> for Probe<'o, 'r, 'c, 'b, 'd> {
    type Output = Result<(), Error>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        match this.phase {
            Phase::Owner => {
                let wake = cx.as_ref().completion();
                if let Err(error) = this.channels.watch_flow(&mut this.owner, &wake) {
                    return task::Poll::Ready(Err(Error::Owner(error)));
                }
                this.phase = Phase::Contender;
                let local = local::Access::new(cx.as_mut().region_token());
                this.connection.control().retire(local, this.target);
                task::Poll::Pending
            }
            Phase::Contender => {
                let wake = cx.as_ref().completion();
                match this.channels.watch_flow(&mut this.contender, &wake) {
                    Ok(()) => return task::Poll::Ready(Err(Error::Reused)),
                    Err(failure::Error::Fanout(fanout::Error::Full)) => {}
                    Err(error) => return task::Poll::Ready(Err(Error::Contender(error))),
                }
                this.owner.clear();
                let wake = cx.as_ref().completion();
                if let Err(error) = this.channels.watch_flow(&mut this.contender, &wake) {
                    return task::Poll::Ready(Err(Error::Held(error)));
                }
                this.phase = Phase::Done;
                let local = local::Access::new(cx.as_mut().region_token());
                this.connection.control().retire(local, this.target);
                task::Poll::Pending
            }
            Phase::Done => task::Poll::Ready(Ok(())),
        }
    }
}
