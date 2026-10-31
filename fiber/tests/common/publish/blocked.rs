use std::task;

use dope::core::driver::schedule::ready::wake;
use dope_amqp091::adapter::{Poll as _, channel, conn, publish};
use dope_amqp091::message::property::Properties;
use dope_amqp091::{adapter, failure};
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use dope_test::alloc::Tracker;
use o3::base::cell::{local, region};

pub struct Blocked<'properties, 'o, 'r, 'c, 'b, 'd> {
    pub(crate) adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, 0>,
    pub(crate) target: conn::Ready<'d, 0>,
    pub(crate) lease: channel::Lease<'c, 'd, 0>,
    pub(crate) properties: &'properties Properties,
}

pub struct Observation<'c, 'b, 'd> {
    pub result: Result<
        task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, 0>>>,
        failure::Runtime<'c, 'b, 'd, 0>,
    >,
    pub allocations: (usize, usize),
}

impl<'properties, 'o, 'r, 'c, 'b, 'd> Blocked<'properties, 'o, 'r, 'c, 'b, 'd> {
    fn attempt(
        &self,
        local: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
        route: &str,
    ) -> Observation<'c, 'b, 'd> {
        let (result, alloc) = Tracker::usage(|| {
            let mut send = self.adapter.publish().send(
                local.read(),
                self.target,
                self.lease,
                publish::Request {
                    exchange: "",
                    routing_key: route,
                    mandatory: false,
                    properties: self.properties,
                    body: b"payload",
                },
            )?;
            Ok(send.poll(local, wake))
        });
        Observation {
            result,
            allocations: alloc,
        }
    }
}

impl<'properties, 'o, 'r, 'c, 'b, 'd> Fiber<'d> for Blocked<'properties, 'o, 'r, 'c, 'b, 'd> {
    type Output = [Observation<'c, 'b, 'd>; 2];

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let first = {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            this.attempt(local, wake, "first")
        };
        let second = {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            this.attempt(local, wake, "second")
        };
        task::Poll::Ready([first, second])
    }
}
