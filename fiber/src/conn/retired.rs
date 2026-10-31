use std::{mem, task};

use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter::conn;
use dope_amqp091::failure;
use dope_fiber::{abi, context};
use o3::base::cell::local;

/// Persistent wait for one exact transport epoch to retire.
#[must_use = "a retirement wait does nothing unless it is driven"]
pub struct Retired<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    observe: conn::Observe<'o, 'r, 'c, 'b, 'd, ID>,
    target: conn::Epoch<'d, ID>,
    wait: fanout::Registration<'o, 'd>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Retired<'o, 'r, 'c, 'b, 'd, ID> {
    pub(crate) const fn new(
        observe: conn::Observe<'o, 'r, 'c, 'b, 'd, ID>,
        target: conn::Epoch<'d, ID>,
    ) -> Self {
        Self {
            observe,
            target,
            wait: fanout::Registration::new(),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> abi::Fiber<'d>
    for Retired<'o, 'r, 'c, 'b, 'd, ID>
{
    type Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (mut retired, mut context) = call.into_parts();
        let wake = context.as_ref().completion();
        let local = local::Access::new(context.as_mut().region_token());
        retired
            .observe
            .poll_retired(local, retired.target, &mut retired.wait, wake)
    }
}

const _: () = assert!(
    mem::size_of::<Retired<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<(
            conn::Observe<'static, 'static, 'static, 'static, 'static, 0>,
            conn::Epoch<'static, 0>,
            fanout::Registration<'static, 'static>,
        )>()
);
