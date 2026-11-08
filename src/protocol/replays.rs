use crate::message::field;
use crate::protocol::method::{self, basic, channel, confirm};
use crate::protocol::topology::records::{self, consumer, exchange, queue};
use crate::protocol::wire::schema::consume;
use crate::protocol::wire::schema::queues::Binding;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, Write, bytes, frame};

pub(crate) struct Queue<'a, 'b, 'd, const ID: u8> {
    record: &'a queue::Declare<'b, 'd>,
    key: queue::Key<'d, ID>,
    current: bool,
}

impl<'a, 'b, 'd, const ID: u8> Queue<'a, 'b, 'd, ID> {
    pub(crate) const fn new(record: &'a queue::Declare<'b, 'd>, key: queue::Key<'d, ID>) -> Self {
        Self {
            record,
            key,
            current: false,
        }
    }

    pub(crate) const fn current(
        record: &'a queue::Declare<'b, 'd>,
        key: queue::Key<'d, ID>,
    ) -> Self {
        Self {
            record,
            key,
            current: true,
        }
    }

    pub(crate) fn token(&self) -> queue::Token<'d, ID> {
        queue::Token::new(
            self.key,
            !self.current && self.record.user_name().is_empty(),
        )
    }
}

impl<'a, 'b, 'd, const ID: u8> method::Spec for Queue<'a, 'b, 'd, ID> {
    const ID: method::Id = {
        use crate::protocol::method::queue::Declare;

        <Declare<'d> as method::Spec>::ID
    };
}

impl<const ID: u8> method::Args for Queue<'_, '_, '_, ID> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        if self.current {
            method::queue::Inspect {
                queue: self.record.server_name(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                arguments: &field::Table::new(),
            }
            .write_args(out);
        } else {
            out.extend(self.record.wire().as_slice());
        }
    }
}

pub(crate) struct QueueBind<'a, 'd, const ID: u8> {
    spec: &'a queue::Bind<'d, ID>,
    queue: short::Short<'a>,
}

impl<'a, 'd, const ID: u8> QueueBind<'a, 'd, ID> {
    pub(crate) const fn new(spec: &'a queue::Bind<'d, ID>, queue: short::Short<'a>) -> Self {
        Self { spec, queue }
    }
}

impl<'a, 'd, const ID: u8> method::Spec for QueueBind<'a, 'd, ID> {
    const ID: method::Id = {
        use crate::protocol::method::queue::Bind;

        <Bind<'d> as method::Spec>::ID
    };
}

impl<const ID: u8> method::Args for QueueBind<'_, '_, ID> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        let mut patch = bytes::Patch::default().with(Binding::QUEUE, self.queue);
        if self.spec.routing_key().is_none() {
            patch = patch.with(Binding::ROUTING_KEY, self.queue);
        }
        self.spec.wire().write_with(out, patch);
    }
}

pub(crate) struct Consume<'a, 'b, 'd, K, const ID: u8> {
    spec: &'a consumer::Record<'b, 'd, ID>,
    queue: short::Short<'a>,
    token: K,
}

impl<'a, 'b, 'd, K: Copy, const ID: u8> Consume<'a, 'b, 'd, K, ID> {
    pub(crate) const fn new(
        spec: &'a consumer::Record<'b, 'd, ID>,
        queue: short::Short<'a>,
        token: K,
    ) -> Self {
        Self { spec, queue, token }
    }

    pub(crate) const fn token(&self) -> K {
        self.token
    }
}

impl<'a, 'b, 'd, K, const ID: u8> method::Spec for Consume<'a, 'b, 'd, K, ID> {
    const ID: method::Id = <basic::Consume<'d> as method::Spec>::ID;
}

impl<K, const ID: u8> method::Args for Consume<'_, '_, '_, K, ID> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        let patch = bytes::Patch::default()
            .with(consume::Layout::QUEUE, self.queue)
            .with(consume::Layout::CONSUMER_TAG, self.spec.tag());
        self.spec.wire().write_with(out, patch);
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Action<'d, K, const ID: u8> {
    Plain(method::Kind),
    Queue(queue::Token<'d, ID>),
    Consumer(K),
}

pub(crate) enum Step<'a, 'b, 'd, K, const ID: u8> {
    Open,
    Close,
    Confirm,
    Qos(records::Qos),
    Exchange(&'a exchange::Declare<'d>),
    Queue(Queue<'a, 'b, 'd, ID>),
    QueueBind(QueueBind<'a, 'd, ID>),
    ExchangeBind(&'a exchange::Bind<'d>),
    Consume(Consume<'a, 'b, 'd, K, ID>),
}

impl<'a, 'b, 'd, K: Copy, const ID: u8> Step<'a, 'b, 'd, K, ID> {
    pub(crate) const fn queue(record: &'a queue::Declare<'b, 'd>, key: queue::Key<'d, ID>) -> Self {
        Self::Queue(Queue::new(record, key))
    }

    pub(crate) const fn current(
        record: &'a queue::Declare<'b, 'd>,
        key: queue::Key<'d, ID>,
    ) -> Self {
        Self::Queue(Queue::current(record, key))
    }

    pub(crate) const fn queue_bind(spec: &'a queue::Bind<'d, ID>, queue: short::Short<'a>) -> Self {
        Self::QueueBind(QueueBind::new(spec, queue))
    }

    pub(crate) const fn consume(
        spec: &'a consumer::Record<'b, 'd, ID>,
        queue: short::Short<'a>,
        token: K,
    ) -> Self {
        Self::Consume(Consume::new(spec, queue, token))
    }

    pub(crate) fn intent(&self) -> Action<'d, K, ID> {
        match self {
            Self::Open => Action::Plain(method::Kind::Open),
            Self::Close => Action::Plain(method::Kind::CloseOk),
            Self::Confirm => Action::Plain(method::Kind::Confirm),
            Self::Qos(_) => Action::Plain(method::Kind::Qos),
            Self::Exchange(_) => Action::Plain(method::Kind::ExchangeDeclare),
            Self::Queue(method) => Action::Queue(method.token()),
            Self::QueueBind(_) => Action::Plain(method::Kind::QueueBind),
            Self::ExchangeBind(_) => Action::Plain(method::Kind::ExchangeBind),
            Self::Consume(method) => Action::Consumer(method.token()),
        }
    }

    pub(crate) fn write<S: Write + AsMut<[u8]> + ?Sized>(&self, out: &mut S, channel: u16) {
        use method::Encode as _;
        use method::exchange::{Bind, Declare};

        match self {
            Self::Open => channel::Open.write(out, channel),
            Self::Close => channel::CloseRef {
                reply_code: 200,
                reply_text: short::Short::EMPTY,
                class_id: 0,
                method_id: 0,
            }
            .write(out, channel),
            Self::Confirm => confirm::Select.write(out, channel),
            Self::Qos(spec) => basic::Qos {
                prefetch_size: spec.prefetch_size,
                prefetch_count: spec.prefetch_count,
                global: spec.global,
            }
            .write(out, channel),
            Self::Exchange(record) => frame::Encoder::new(out).method_with(
                channel,
                <Declare<'d> as method::Spec>::ID.bytes(),
                |out| out.extend(record.wire().as_slice()),
            ),
            Self::Queue(method) => method.write(out, channel),
            Self::QueueBind(method) => method.write(out, channel),
            Self::ExchangeBind(record) => frame::Encoder::new(out).method_with(
                channel,
                <Bind<'d> as method::Spec>::ID.bytes(),
                |out| out.extend(record.wire().as_slice()),
            ),
            Self::Consume(method) => method.write(out, channel),
        }
    }
}
