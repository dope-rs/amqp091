use std::pin::Pin;
use std::task::Poll;

use dope_fiber::abi::{Fiber, PollFn, Task};
use dope_fiber::context;

pub struct Pinned<F>(Pin<Box<Task<F>>>);

impl<F> Pinned<F> {
    pub fn new(fiber: F) -> Self {
        Self(Box::pin(Task::new(fiber)))
    }

    pub fn as_pin(&mut self) -> Pin<&mut Task<F>> {
        self.0.as_mut()
    }
}

impl<'d, F> Fiber<'d> for Pinned<F>
where
    F: Fiber<'d>,
{
    type Output = F::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        admit.poll(this.get_mut().0.as_mut()).0
    }
}

/// Polls a fiber exactly once and returns that poll without driving it again.
///
/// This is useful for deterministic test barriers: a waiter can be registered
/// before the scripted peer is allowed to publish the event it waits for.
pub fn poll_once<'a, 'd, F>(
    mut fiber: Pin<&'a mut Task<F>>,
) -> impl Fiber<'d, Output = Poll<F::Output>> + 'a
where
    F: Fiber<'d> + 'a,
    'd: 'a,
{
    PollFn::admitted(move |admit| Poll::Ready(admit.poll(fiber.as_mut()).0))
}
