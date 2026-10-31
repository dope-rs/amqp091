use dope_amqp091::failure;
use dope_amqp091::protocol::method::tx;
use dope_fiber::abi;

use crate::channel;

pub struct Transaction<
    'a,
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    A = (),
> {
    inner: &'a channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'a, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, A>
    Transaction<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
where
    A: channel::Scope<'d, ID>,
{
    pub(super) const fn new(
        inner: &'a channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>,
    ) -> Self {
        Self { inner }
    }

    pub fn select(
        self,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'a {
        self.inner.call(tx::Select)
    }

    pub fn commit(
        self,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'a {
        self.inner.call(tx::Commit)
    }

    pub fn rollback(
        self,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'a {
        self.inner.call(tx::Rollback)
    }
}

impl<B: ?Sized, const ID: u8, A> Copy for Transaction<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {}
impl<B: ?Sized, const ID: u8, A> Clone for Transaction<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {
    fn clone(&self) -> Self {
        *self
    }
}
