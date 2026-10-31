use std::ops;

use dope::runtime::client;
use dope_amqp091::adapter::consumer;
use dope_amqp091::message::ack;

use crate::channel::consumer::Settle;

/// A delivery carrying the exact connection and channel authority that issued
/// its delivery tag.
///
/// Auto-acknowledged deliveries carry no settlement authority.
pub struct Delivery<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode = ack::Manual,
> {
    inner: consumer::Delivery<'o, 'r, 'c, 'b, 'd, ID, M>,
    bound: client::Bound<'app, 'd, B>,
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode,
> Delivery<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>
{
    pub(super) const fn new(
        inner: consumer::Delivery<'o, 'r, 'c, 'b, 'd, ID, M>,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self {
        Self { inner, bound }
    }

    /// Releases the payload lease without settling its delivery tag.
    pub fn release(self) {
        self.inner.release();
    }
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode,
> ops::Deref for Delivery<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>
{
    type Target = consumer::Delivery<'o, 'r, 'c, 'b, 'd, ID, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Delivery<'app, 'o, 'r, 'c, 'b, 'd, B, ID, ack::Manual>
{
    /// Consumes both the payload lease and its exact settlement authority.
    pub fn settle(self, kind: ack::Disposition) -> Settle<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        let Self { inner, bound } = self;
        Settle::new(inner.settle(kind), bound)
    }
}

const _: () = {
    assert!(
        size_of::<Delivery<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Manual>>(
        ) == size_of::<
            consumer::Delivery<'static, 'static, 'static, 'static, 'static, 0, ack::Manual>,
        >()
    );
    assert!(
        size_of::<Delivery<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Manual>>(
        ) == 5 * size_of::<usize>()
    );
    assert!(
        size_of::<Delivery<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Auto>>(
        ) == size_of::<consumer::Delivery<'static, 'static, 'static, 'static, 'static, 0, ack::Auto>>(
        )
    );
    assert!(
        size_of::<Delivery<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Auto>>(
        ) == size_of::<usize>()
    );
};
