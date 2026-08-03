use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use dope::driver::timer::Timer;
use dope_fiber::abi::Fiber;
use dope_fiber::abi::pollfn::PollFn;
use dope_fiber::abi::race::{Either, Race};
use dope_fiber::raw::task::Context;
use dope_fiber::sleep::TimerExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed;

impl Display for Elapsed {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("test deadline elapsed")
    }
}

impl Error for Elapsed {}

pub struct PinnedFiber<F>(Pin<Box<F>>);

impl<F> PinnedFiber<F> {
    pub fn new(fiber: F) -> Self {
        Self(Box::pin(fiber))
    }

    pub fn as_pin(&mut self) -> Pin<&mut F> {
        self.0.as_mut()
    }
}

impl<'d, F> Fiber<'d> for PinnedFiber<F>
where
    F: Fiber<'d>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

/// Bounds a fiber by a timer owned by the same runtime domain.
///
/// Dropping the losing side of the race cancels its timer ticket or pending
/// operation through the ordinary [`Fiber`] drop path.
#[dope_fiber::fiber_fn('d)]
pub async fn within<'a, 'd, F>(
    timer: &'a Timer<'d>,
    duration: Duration,
    fiber: F,
) -> Result<F::Output, Elapsed>
where
    F: Fiber<'d> + 'a,
{
    match Race::new(fiber, timer.sleep(duration)).await {
        Either::Left(output) => Ok(output),
        Either::Right(()) => Err(Elapsed),
    }
}

/// Polls a fiber exactly once and returns that poll without driving it again.
///
/// This is useful for deterministic test barriers: a waiter can be registered
/// before the scripted peer is allowed to publish the event it waits for.
pub fn poll_once<'a, 'd, F>(
    mut fiber: Pin<&'a mut F>,
) -> impl Fiber<'d, Output = Poll<F::Output>> + 'a
where
    F: Fiber<'d> + 'a,
    'd: 'a,
{
    PollFn::new(move |cx| Poll::Ready(Fiber::poll(fiber.as_mut(), cx)))
}

/// Polls a fiber exactly once and fails the test if it completes immediately.
#[dope_fiber::fiber_fn('d)]
pub async fn expect_pending<'a, 'd, F>(fiber: Pin<&'a mut F>)
where
    F: Fiber<'d> + 'a,
    'd: 'a,
{
    let outcome = poll_once(fiber).await;
    assert!(
        outcome.is_pending(),
        "fiber completed before the test barrier"
    );
}
