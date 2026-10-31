use dope_amqp091::adapter::publish;
use dope_amqp091::failure;
use dope_amqp091::protocol::method::confirm;
use dope_amqp091::protocol::{self};
use dope_fiber::abi;

use crate::channel;
use crate::runtime::channel::send;
use crate::runtime::ready;

pub struct Select<
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

pub struct Selected<
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
    Select<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
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
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            Selected<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    > + use<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        dope_fiber::fiber!('d => async move {
            self.inner.call(confirm::Select).await?;
            Ok(Selected { inner: self.inner })
        })
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'a, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, A>
    Selected<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
where
    A: channel::Scope<'d, ID>,
{
    pub fn publish<'exchange, 'key, 'properties, 'body>(
        self,
        req: publish::Request<'exchange, 'key, 'properties, 'body>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<protocol::Confirmation, failure::Runtime<'c, 'b, 'd, ID>>,
    > + use<'exchange, 'key, 'properties, 'body, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        ready::Op::new(
            self.inner.access.source(self.inner.handle),
            send::Send::<true, ID>::new(self.inner.lease, req),
        )
    }
}

const _: () = {
    use std::mem;

    assert!(
        mem::size_of::<Select<'static, 'static, 'static, 'static, 'static, 'static, 'static, (), 0>>(
        ) == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            Selected<'static, 'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
        >() == mem::size_of::<usize>()
    );
};

impl<B: ?Sized, const ID: u8, A> Copy for Select<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {}
impl<B: ?Sized, const ID: u8, A> Clone for Select<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: ?Sized, const ID: u8, A> Copy for Selected<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {}
impl<B: ?Sized, const ID: u8, A> Clone for Selected<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {
    fn clone(&self) -> Self {
        *self
    }
}
