use std::{mem, task};

use dope::runtime::client;
use dope_amqp091::adapter::consumer;
use dope_amqp091::failure;
use dope_amqp091::message::ack;
use dope_fiber::{abi, context};
use o3::base::cell::local;

use crate::channel::consumer::Delivery;

pub struct Recv<
    'recv,
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode,
> {
    inner: consumer::Recv<'recv, 'o, 'r, 'c, 'b, 'd, ID, M>,
    bound: client::Bound<'app, 'd, B>,
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'recv,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode,
> Recv<'recv, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>
{
    pub(crate) const fn new(
        inner: consumer::Recv<'recv, 'o, 'r, 'c, 'b, 'd, ID, M>,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self {
        Self { inner, bound }
    }
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'recv,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode,
> abi::Fiber<'d> for Recv<'recv, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>
{
    type Output = Result<
        Option<Delivery<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>>,
        failure::Runtime<'c, 'b, 'd, ID>,
    >;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (mut fiber, mut context) = call.into_parts();
        let wake = context.as_ref().waker();
        let local = local::Access::new(context.as_mut().region_token());
        let this = fiber.as_mut().get_mut();
        match this.inner.poll(local, || wake.completion()) {
            task::Poll::Ready(Ok(Some(delivery))) => {
                task::Poll::Ready(Ok(Some(Delivery::new(delivery, this.bound))))
            }
            task::Poll::Ready(Ok(None)) => task::Poll::Ready(Ok(None)),
            task::Poll::Ready(Err(error)) => task::Poll::Ready(Err(error)),
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<
            Recv<'static, 'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Manual>,
        >() == mem::size_of::<
            consumer::Recv<'static, 'static, 'static, 'static, 'static, 'static, 0, ack::Manual>,
        >()
    );
};
