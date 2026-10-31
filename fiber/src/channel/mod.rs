use dope_amqp091::adapter::channel::{Lease, Returned};
use dope_amqp091::adapter::conn;
use dope_amqp091::failure;
use dope_amqp091::protocol::method::Op;
use dope_amqp091::protocol::method::channel::CloseRef;
use dope_amqp091::protocol::wire::text::short;
use dope_fiber::abi;

use crate::runtime::channel::method;
use crate::runtime::{self, ready};

pub(crate) mod access;
pub mod basic;
pub mod confirm;
pub mod consumer;
pub mod exchange;
pub mod queue;
pub mod tx;
mod waits;
pub(crate) use access::Route;

pub trait Scope<'d, const ID: u8>: Route<'d, ID> {}

impl<'d, const ID: u8> Scope<'d, ID> for () {}

impl<'d, const ID: u8> Scope<'d, ID> for conn::Ready<'d, ID> {}

pub struct Channel<
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
    pub(super) handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    pub(super) lease: Lease<'c, 'd, ID>,
    pub(super) access: A,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, A>
    Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
where
    A: Scope<'d, ID>,
{
    pub fn id(&self) -> u16 {
        self.lease.id()
    }

    pub fn basic(&self) -> basic::Basic<'_, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        basic::Basic::new(self)
    }

    pub fn confirm(&self) -> confirm::Select<'_, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        confirm::Select::new(self)
    }

    pub fn exchange(&self) -> exchange::Exchange<'_, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        exchange::Exchange::new(self)
    }

    pub fn queue(&self) -> queue::Queue<'_, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        queue::Queue::new(self)
    }

    pub fn transaction(&self) -> tx::Transaction<'_, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        tx::Transaction::new(self)
    }

    pub(super) fn call<M>(
        &self,
        value: M,
    ) -> ready::Op<
        'app,
        'o,
        'r,
        'c,
        'b,
        'd,
        B,
        ID,
        A::Source<'app, 'o, 'r, 'c, 'b, B>,
        method::Call<'c, 'd, ID, M>,
    >
    where
        M: Op<'d>,
    {
        ready::Op::new(
            self.access.source(self.handle),
            method::Call::new(self.lease, value),
        )
    }

    pub fn close<'a>(
        &'a self,
        code: u16,
        text: &'a str,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'a {
        dope_fiber::fiber!('d => async move {
            let text = short::Short::new(text)?;
            self.call(CloseRef {
                    reply_code: code,
                    reply_text: text,
                    class_id: 0,
                    method_id: 0,
                })
            .await
        })
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub fn flow(
        &self,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + '_ {
        waits::Flow::new(self.handle, self.lease)
    }

    /// Only one returned-message receiver may be pending per channel.
    /// A second receiver fails with `failure::Error::Table` without replacing the first.
    pub fn next_return(
        &self,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Option<Returned<'c, 'b, 'd>>, failure::Runtime<'c, 'b, 'd, ID>>,
    > + '_ {
        waits::NextReturn::new(self.handle, self.lease)
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, conn::Ready<'d, ID>>
{
    pub fn into_auto(self) -> Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        Channel {
            handle: self.handle,
            lease: self.lease,
            access: (),
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Channel<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == std::mem::size_of::<(
            runtime::Handle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
            Lease<'static, 'static, 0>,
        )>()
);
const _: () = assert!(
    std::mem::size_of::<
        Channel<
            'static,
            'static,
            'static,
            'static,
            'static,
            'static,
            (),
            0,
            conn::Ready<'static, 0>,
        >,
    >() == std::mem::size_of::<
        runtime::Handle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
    >() + std::mem::size_of::<Lease<'static, 'static, 0>>()
        + std::mem::size_of::<conn::Ready<'static, 0>>()
);
