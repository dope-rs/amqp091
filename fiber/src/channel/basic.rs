use dope_amqp091::adapter::{consumer, publish};
use dope_amqp091::failure;
use dope_amqp091::message::{ack, field};
use dope_amqp091::protocol::method::basic;
use dope_amqp091::protocol::wire::text::short::{self, Resource};
use dope_amqp091::protocol::{self};
use dope_fiber::abi;

use crate::channel::consumer::Consumer;
use crate::channel::{self};
use crate::runtime::channel::{Bind, send};
use crate::runtime::ready;

pub struct Basic<
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

pub struct Consume<'a, M = ack::Manual> {
    pub queue: &'a str,
    pub consumer_tag: &'a str,
    pub no_local: bool,
    pub ack: M,
    pub exclusive: bool,
    pub arguments: &'a field::Table,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'a, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, A>
    Basic<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
where
    A: channel::Scope<'d, ID>,
{
    pub(super) const fn new(
        inner: &'a channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>,
    ) -> Self {
        Self { inner }
    }

    pub fn qos(
        self,
        count: u16,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'a {
        self.inner.call(basic::Qos {
            prefetch_size: 0,
            prefetch_count: count,
            global: false,
        })
    }

    pub fn consume<'input, M>(
        self,
        req: Consume<'input, M>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            Consumer<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    > + 'input
    where
        'a: 'input,
        M: consumer::Mode + 'input,
    {
        dope_fiber::fiber!('d => async move {
            let queue = short::Short::resource(req.queue, Resource::Queue)?;
            let tag = short::Short::new(req.consumer_tag)?;
            let (handle, ready, lease, registered): (
                _,
                _,
                _,
                protocol::Consumer<'b, 'd, _, M>,
            ) = ready::Op::new(
                self.inner.access.source(self.inner.handle),
                Bind::new(
                    self.inner.lease,
                    basic::Consume {
                        queue,
                        consumer_tag: tag,
                        no_local: req.no_local,
                        ack: req.ack,
                        exclusive: req.exclusive,
                        arguments: req.arguments,
                    },
                ),
            )
            .await?;
            let method = registered.method;
            let registered_tag = method.consumer_tag;
            Consumer::try_new(
                handle,
                ready,
                lease,
                registered.key,
                registered_tag,
            )
        })
    }

    pub fn publish<'exchange, 'key, 'properties, 'body>(
        self,
        req: publish::Request<'exchange, 'key, 'properties, 'body>,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>>
    + use<'exchange, 'key, 'properties, 'body, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A> {
        ready::Op::new(
            self.inner.access.source(self.inner.handle),
            send::Send::<false, ID>::new(self.inner.lease, req),
        )
    }
}

impl<B: ?Sized, const ID: u8, A> Copy for Basic<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {}
impl<B: ?Sized, const ID: u8, A> Clone for Basic<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {
    fn clone(&self) -> Self {
        *self
    }
}
