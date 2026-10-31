use std::task;

use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter::channel;
use dope_amqp091::failure;
use dope_fiber::{abi, context};
use o3::base::cell::local;

use crate::runtime;

pub(crate) struct Flow<
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
    progress: fanout::Registration<'o, 'd>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Flow<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(in crate::channel) fn new(
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        lease: channel::Lease<'c, 'd, ID>,
    ) -> Self {
        Self {
            handle,
            lease,
            state: fanout::Registration::new(),
            progress: fanout::Registration::new(),
        }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8> abi::Fiber<'d>
    for Flow<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    type Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (self_, mut cx) = call.into_parts();
        let this = self_.get_mut();
        let conn = this.handle.adapter;
        let wake = cx.as_ref().completion();
        let mut local = local::Access::new(cx.as_mut().region_token());
        let ready = task::ready!(conn.connection().observe().poll_ready(
            local.reborrow(),
            &mut this.state,
            wake,
        ))?;
        let active = match conn.channels().flow_active(local, ready, this.lease) {
            Ok(Some(active)) => active,
            Ok(None) => {
                return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Closed)));
            }
            Err(error) => return task::Poll::Ready(Err(error)),
        };
        if active {
            task::Poll::Ready(Ok(()))
        } else {
            match conn
                .channels()
                .watch_flow(&mut this.progress, &cx.as_ref().completion())
            {
                Ok(()) => task::Poll::Pending,
                Err(error) => task::Poll::Ready(Err(failure::Runtime::from(error))),
            }
        }
    }
}
