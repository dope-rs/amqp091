use std::{mem, task};

use dope::runtime::client;
use dope_amqp091::adapter::consumer::{self, Error};
use dope_fiber::{abi, context};
use o3::base::cell::local;

/// One-shot operation bound to the delivery's exact source generation.
#[repr(transparent)]
pub struct Settle<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8> {
    settle: client::Bound<'app, 'd, B, consumer::Settle<'o, 'r, 'c, 'b, 'd, ID>>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Settle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(super) const fn new(
        settle: consumer::Settle<'o, 'r, 'c, 'b, 'd, ID>,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self {
        Self {
            settle: bound.bind(settle),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, B: ?Sized, const ID: u8> abi::Fiber<'d>
    for Settle<'_, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    type Output = Result<(), Error<'c, 'b, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (mut fiber, mut context) = call.into_parts();
        let this = fiber.as_mut().get_mut();
        let wake = context.as_ref().completion();
        let local = local::Access::new(context.as_mut().region_token());
        this.settle.poll(local, wake)
    }
}

const _: () = assert!(
    mem::size_of::<Settle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == mem::size_of::<consumer::Settle<'static, 'static, 'static, 'static, 'static, 0>>()
);
const _: () = assert!(
    mem::align_of::<Settle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == mem::align_of::<consumer::Settle<'static, 'static, 'static, 'static, 'static, 0>>()
);
const _: () = assert!(
    mem::size_of::<Option<Settle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>>()
        == mem::size_of::<Option<consumer::Settle<'static, 'static, 'static, 'static, 'static, 0>>>(
        )
);
const _: () = assert!(
    mem::needs_drop::<Settle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == mem::needs_drop::<consumer::Settle<'static, 'static, 'static, 'static, 'static, 0>>()
);
