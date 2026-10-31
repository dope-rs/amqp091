use std::{mem, task};

use dope::runtime::client;
use dope_amqp091::adapter::{Poll as _, conn};
use dope_amqp091::{adapter, failure};
use dope_fiber::{abi, context};
use o3::base::cell::{local, region};

use crate::runtime;
use crate::runtime::ready::Source;

pub(crate) trait Start<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
>
{
    type Output;
    type Run: adapter::Poll<'c, 'b, 'd, ID, Output = Self::Output>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>>;
}

enum Stage<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8, S, K>
where
    K: Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
{
    Start(S, K),
    Run(K::Run),
    Done,
}

pub(crate) struct Op<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    S,
    K,
> where
    K: Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
{
    stage: client::Bound<'app, 'd, B, Stage<'app, 'o, 'r, 'c, 'b, 'd, B, ID, S, K>>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, S, K>
    Op<'app, 'o, 'r, 'c, 'b, 'd, B, ID, S, K>
where
    S: Source<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    K: Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
{
    pub(crate) fn new(source: S, start: K) -> Self {
        let bound = source.handle().bound();
        Self {
            stage: bound.bind(Stage::Start(source, start)),
        }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, S, K> Unpin
    for Op<'app, 'o, 'r, 'c, 'b, 'd, B, ID, S, K>
where
    K: Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
{
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, S, K>
    abi::Fiber<'d> for Op<'app, 'o, 'r, 'c, 'b, 'd, B, ID, S, K>
where
    S: Source<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    K: Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
{
    type Output = Result<K::Output, failure::Runtime<'c, 'b, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (fiber, mut context) = call.into_parts();
        let this = fiber.get_mut();
        let stage = mem::replace(&mut *this.stage, Stage::Done);
        let mut run = match stage {
            Stage::Start(mut source, start) => {
                let ready = match source.poll(context.as_mut()) {
                    task::Poll::Pending => {
                        *this.stage = Stage::Start(source, start);
                        return task::Poll::Pending;
                    }
                    task::Poll::Ready(Ok(ready)) => ready,
                    task::Poll::Ready(Err(error)) => return task::Poll::Ready(Err(error)),
                };
                let handle = source.handle();
                let access = local::Access::new(context.as_mut().region_token());
                match start.start(handle, access, ready) {
                    Ok(run) => run,
                    Err(error) => return task::Poll::Ready(Err(error)),
                }
            }
            Stage::Run(run) => run,
            Stage::Done => {
                return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
            }
        };
        let wake = context.as_ref().completion();
        let access = local::Access::new(context.as_mut().region_token());
        match run.poll(access, wake) {
            task::Poll::Pending => {
                *this.stage = Stage::Run(run);
                task::Poll::Pending
            }
            task::Poll::Ready(result) => task::Poll::Ready(result),
        }
    }
}
