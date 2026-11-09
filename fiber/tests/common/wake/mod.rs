use std::net::SocketAddr;
use std::task;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::core::driver::schedule::ready::wake::fanout::Registration;
use dope::manifold::timing;
use dope_amqp091::adapter::channel::Lease;
use dope_amqp091::adapter::{Poll as _, conn, method};
use dope_amqp091::protocol::method::channel;
use dope_amqp091::{adapter, config, failure};
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use o3::base::cell::local;

use crate::common::amqp::Tune;
use crate::common::amqp::app::{Endpoint, Policy, Scope, error};

pub const TUNE: Tune = Tune::new(16, 131_072, 0);

#[derive(Debug)]
pub enum Fault<E> {
    Runtime(E),
    Allocations { count: usize, bytes: usize },
}

pub fn with_client(
    addr: SocketAddr,
    slots: usize,
    test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
        &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, timing::Throughput>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<(), error::Error<'owner, ()>>),
) {
    let cfg = match config::Config::new("guest", "guest") {
        Ok(config) => config,
        Err(error) => return finish(Err(error::Error::Config(error))),
    };
    let policy = Policy::new(1, slots, Duration::from_secs(10));
    Endpoint::new(addr, cfg).try_check::<timing::Throughput, _>(policy, test, recover, finish)
}

pub struct Ready<'o, 'r, 'c, 'b, 'd> {
    pub(crate) state: conn::Observe<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) wait: Registration<'o, 'd>,
}

impl<'o, 'r, 'c, 'b, 'd> Fiber<'d> for Ready<'o, 'r, 'c, 'b, 'd> {
    type Output = Result<conn::Ready<'d, 0>, Fault<failure::Runtime<'c, 'b, 'd, 0>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let wake = cx.as_ref().completion();
        let local = local::Access::new(cx.as_mut().region_token());
        this.state
            .poll_ready(local, &mut this.wait, wake)
            .map_err(Fault::Runtime)
    }
}

pub struct Open<'o, 'r, 'c, 'b, 'd> {
    pub(crate) adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) target: conn::Ready<'d, 0>,
    pub(crate) lease: Option<Lease<'c, 'd, 0>>,
    pub(crate) call: Option<method::Call<'o, 'r, 'c, 'b, 'd, 0, channel::Open>>,
}

impl<'o, 'r, 'c, 'b, 'd> Fiber<'d> for Open<'o, 'r, 'c, 'b, 'd> {
    type Output = Result<Lease<'c, 'd, 0>, failure::Runtime<'c, 'b, 'd, 0>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let wake = cx.as_ref().completion();
        let mut local = local::Access::new(cx.as_mut().region_token());
        let lease = match &mut this.lease {
            Some(lease) => *lease,
            slot @ None => *slot.insert(
                this.adapter
                    .channels()
                    .reserve(local.reborrow(), this.target, 1)?
                    .commit(),
            ),
        };
        let call = match &mut this.call {
            Some(call) => call,
            slot @ None => slot.insert(this.adapter.methods().reserve(
                local.reborrow(),
                this.target,
                lease,
                channel::Open,
            )?),
        };
        call.poll(local, wake).map(|result| result.map(|()| lease))
    }
}

mod flow;
pub use flow::Flow;

pub mod progress;

pub mod probe;

mod purge;
pub use purge::Purge;
