use dope_amqp091::failure;
use dope_amqp091::message::field;
use dope_amqp091::protocol::method::queue;
use dope_amqp091::protocol::wire::text::{resident, short};
use dope_fiber::abi;

use crate::channel;

pub struct Queue<
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
    pub queue: &'a str,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: &'a field::Table,
}

pub struct Inspect<'a> {
    pub queue: &'a str,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: &'a field::Table,
}

pub struct Declared<'b, 'd: 'b> {
    pub queue: resident::Short<'b, 'd>,
    pub message_count: u32,
    pub consumer_count: u32,
}

#[derive(Debug)]
pub struct Purged {
    pub message_count: u32,
}

pub struct Deleted {
    pub message_count: u32,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'a, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, A>
    Queue<'a, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
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
    ) -> impl abi::Fiber<'d, Output = Result<Declared<'b, 'd>, failure::Runtime<'c, 'b, 'd, ID>>> + 'input
    where
        'a: 'input,
    {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(options.queue, short::Resource::Queue)?;
            let method = self.inner
                .call(queue::Declare {
                    queue: name,
                    durable: options.durable,
                    exclusive: options.exclusive,
                    auto_delete: options.auto_delete,
                    arguments: options.arguments,
                })
                .await?;
            Ok(Declared {
                queue: method.queue,
                message_count: method.message_count,
                consumer_count: method.consumer_count,
            })
        })
    }

    pub fn inspect<'input>(
        self,
        options: Inspect<'input>,
    ) -> impl abi::Fiber<'d, Output = Result<Declared<'b, 'd>, failure::Runtime<'c, 'b, 'd, ID>>> + 'input
    where
        'a: 'input,
    {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(options.queue, short::Resource::Queue)?;
            let method = self.inner
                .call(queue::Inspect {
                    queue: name,
                    durable: options.durable,
                    exclusive: options.exclusive,
                    auto_delete: options.auto_delete,
                    arguments: options.arguments,
                })
                .await?;
            Ok(Declared {
                queue: method.queue,
                message_count: method.message_count,
                consumer_count: method.consumer_count,
            })
        })
    }

    pub fn bind<'input>(
        self,
        name: &'input str,
        exchange: &'input str,
        key: &'input str,
        args: &'input field::Table,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'input
    where
        'a: 'input,
    {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(name, short::Resource::Queue)?;
            let exchange = short::Short::resource(exchange, short::Resource::Exchange)?;
            let key = short::Short::new(key)?;
            self.inner
                .call(queue::Bind {
                    queue: name,
                    exchange,
                    routing_key: key,
                    arguments: args,
                })
                .await
        })
    }

    pub fn unbind(
        self,
        name: &'a str,
        exchange: &'a str,
        key: &'a str,
        args: &'a field::Table,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'a {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(name, short::Resource::Queue)?;
            let exchange = short::Short::resource(exchange, short::Resource::Exchange)?;
            let key = short::Short::new(key)?;
            self.inner
                .call(queue::Unbind {
                    queue: name,
                    exchange,
                    routing_key: key,
                    arguments: args,
                })
                .await
        })
    }

    pub fn purge<'input>(
        self,
        name: &'input str,
    ) -> impl abi::Fiber<'d, Output = Result<Purged, failure::Runtime<'c, 'b, 'd, ID>>> + 'input
    where
        'a: 'input,
    {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(name, short::Resource::Queue)?;
            let method = self
                .inner
                .call(queue::Purge { queue: name })
                .await?;
            Ok(Purged {
                message_count: method.message_count,
            })
        })
    }

    pub fn delete<'input>(
        self,
        name: &'input str,
        unused: bool,
        empty: bool,
    ) -> impl abi::Fiber<'d, Output = Result<Deleted, failure::Runtime<'c, 'b, 'd, ID>>> + 'input
    where
        'a: 'input,
    {
        dope_fiber::fiber!('d => async move {
            let name = short::Short::resource(name, short::Resource::Queue)?;
            let method = self
                .inner
                .call(queue::Delete {
                    queue: name,
                    if_unused: unused,
                    if_empty: empty,
                })
                .await?;
            Ok(Deleted {
                message_count: method.message_count,
            })
        })
    }
}

impl<B: ?Sized, const ID: u8, A> Copy for Queue<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {}
impl<B: ?Sized, const ID: u8, A> Clone for Queue<'_, '_, '_, '_, '_, '_, '_, B, ID, A> {
    fn clone(&self) -> Self {
        *self
    }
}
