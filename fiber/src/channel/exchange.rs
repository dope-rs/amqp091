use dope_amqp091::failure;
use dope_amqp091::message::field;
use dope_amqp091::protocol::method::exchange;
use dope_amqp091::protocol::wire::text::short;
use dope_fiber::abi;

use crate::channel;

pub struct Exchange<
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

pub struct Declare<'a> {
    pub exchange: &'a str,
    pub kind: &'a str,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: &'a field::Table,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'a, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, A>
    Exchange<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
where
    A: channel::Scope<'d, ID>,
{
    pub(super) const fn new(
        inner: &'a channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>,
    ) -> Self {
        Self { inner }
    }

    pub fn declare<'input>(
        self,
        options: Declare<'input>,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'input
    where
        'a: 'input,
    {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(options.exchange, short::Resource::Exchange)?;
            let kind = short::Short::new(options.kind)?;
            self.inner
                .call(exchange::Declare {
                    exchange: name,
                    kind,
                    durable: options.durable,
                    auto_delete: options.auto_delete,
                    internal: options.internal,
                    arguments: options.arguments,
                })
                .await
        })
    }
}

impl<B: ?Sized, const ID: u8, A> Copy for Exchange<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {}
impl<B: ?Sized, const ID: u8, A> Clone for Exchange<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {
    fn clone(&self) -> Self {
        *self
    }
}
