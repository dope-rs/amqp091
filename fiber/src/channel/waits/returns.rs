use std::task;

use dope::core::driver::schedule::ready::wake::{fanout, table};
use dope_amqp091::adapter::channel;
use dope_amqp091::failure;
use dope_fiber::{abi, context};
use o3::base::cell::local;

use crate::runtime;

pub(crate) struct NextReturn<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    lease: channel::Lease<'c, 'd, ID>,
    state: fanout::Registration<'o, 'd>,
    wait: table::Registration<'o, 'd>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    NextReturn<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(in crate::channel) fn new(
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        lease: channel::Lease<'c, 'd, ID>,
    ) -> Self {
        Self {
            handle,
            lease,
            state: fanout::Registration::new(),
            wait: table::Registration::new(),
        }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8> abi::Fiber<'d>
    for NextReturn<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    type Output = Result<Option<channel::Returned<'c, 'b, 'd>>, failure::Runtime<'c, 'b, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (fiber, mut cx) = call.into_parts();
        let this = fiber.get_mut();
        let conn = this.handle.adapter;
        let wake = cx.as_ref().waker();
        let mut local = local::Access::new(cx.as_mut().region_token());
        let target = match task::ready!(conn.connection().observe().poll_ready(
            local.reborrow(),
            &mut this.state,
            wake.completion(),
        )) {
            Ok(target) => target,
            Err(error) => {
                this.wait.clear();
                return task::Poll::Ready(Err(error));
            }
        };
        let wake = wake.completion();
        match conn
            .channels()
            .next_return(local, target, this.lease, &mut this.wait, wake)
        {
            Ok(None) => task::Poll::Pending,
            output => task::Poll::Ready(output),
        }
    }
}
