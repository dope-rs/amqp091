use o3::buffer::hold::scope;
use o3::buffer::{self};

use crate::failure;
use crate::failure::wire::{Allocation, Fault};
use crate::message::field;
use crate::protocol::wire::schema::queues::{Binding, Declaration};
use crate::protocol::wire::schema::{self, resource};
use crate::protocol::wire::text::{fields, resident, short};
use crate::protocol::wire::{Put as _, outputs, reader};
use crate::protocol::{self, class, method, wire};

pub(super) const DECLARE: method::Id = method::Id::new(class::QUEUE, 10);
pub(super) const DECLARE_OK: method::Id = method::Id::new(class::QUEUE, 11);
pub(super) const BIND: method::Id = method::Id::new(class::QUEUE, 20);
pub(super) const BIND_OK: method::Id = method::Id::new(class::QUEUE, 21);
pub(super) const PURGE: method::Id = method::Id::new(class::QUEUE, 30);
pub(super) const PURGE_OK: method::Id = method::Id::new(class::QUEUE, 31);
pub(super) const DELETE: method::Id = method::Id::new(class::QUEUE, 40);
pub(super) const DELETE_OK: method::Id = method::Id::new(class::QUEUE, 41);
pub(super) const UNBIND: method::Id = method::Id::new(class::QUEUE, 50);
pub(super) const UNBIND_OK: method::Id = method::Id::new(class::QUEUE, 51);

/// Declares a queue and records it for recovery.
pub struct Declare<'a> {
    pub queue: short::Name<'a>,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: &'a field::Table,
}

impl method::Spec for Declare<'_> {
    const ID: method::Id = DECLARE;
}

impl method::Args for Declare<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_declare(
            out,
            self.queue,
            false,
            self.durable,
            self.exclusive,
            self.auto_delete,
            self.arguments,
        );
    }
}

impl method::Request for Declare<'_> {
    type Response = DeclareReply;
}

impl<'d> method::Op<'d> for Declare<'_> {
    type Output<'b, Consumer> = DeclareOk<'b, 'd>;
}

pub struct Inspect<'a> {
    pub queue: short::Name<'a>,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: &'a field::Table,
}

impl method::Spec for Inspect<'_> {
    const ID: method::Id = DECLARE;
}

impl method::Args for Inspect<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_declare(
            out,
            self.queue,
            true,
            self.durable,
            self.exclusive,
            self.auto_delete,
            self.arguments,
        );
    }
}

impl method::Request for Inspect<'_> {
    type Response = DeclareReply;
}

impl<'d> method::Op<'d> for Inspect<'_> {
    type Output<'b, Consumer> = DeclareOk<'b, 'd>;
}

fn write_declare<S>(
    out: &mut S,
    queue: short::Name<'_>,
    probe: bool,
    disk: bool,
    sole: bool,
    auto: bool,
    args: &field::Table,
) where
    S: wire::Put,
{
    out.put_u16(0);
    out.put_name(queue);
    out.put_bits([probe, disk, sole, auto, false]);
    args.encode(out);
}

impl method::Record<Declaration> for Declare<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, Declaration>,
    ) -> outputs::Fields<'a, Declaration, [schema::Offset; 1]> {
        out.put_u16(0);
        let mut out = out.put_field(Declaration::QUEUE, self.queue.into());
        out.put_bits([false, self.durable, self.exclusive, self.auto_delete, false]);
        out.put_table(self.arguments);
        out
    }
}

pub struct DeclareOk<'b, 'd> {
    pub queue: resident::Short<'b, 'd>,
    pub message_count: u32,
    pub consumer_count: u32,
}

pub(crate) struct View<'input> {
    queue: short::Short<'input>,
    pub message_count: u32,
    pub consumer_count: u32,
}

impl method::Spec for DeclareOk<'_, '_> {
    const ID: method::Id = DECLARE_OK;
}

#[doc(hidden)]
pub enum DeclareReply {}

impl method::Spec for DeclareReply {
    const ID: method::Id = DECLARE_OK;
}

impl method::Reply for DeclareReply {
    const KIND: method::Kind = method::Kind::QueueDeclare;
    type Output<'b, 'd, Consumer> = DeclareOk<'b, 'd>;
    type Value<'b, 'd, Consumer> = DeclareOk<'b, 'd>;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Self::Output<'b, 'd, Consumer>, failure::Error> {
        let actual = reply.kind();
        match reply {
            protocol::Response::QueueDeclareOk(ok) => Ok(ok),
            _ => Err(failure::Error::Protocol(Fault::Response {
                expected: method::Kind::QueueDeclare,
                actual,
            })),
        }
    }
}

impl<'input> View<'input> {
    pub(crate) fn read<'d, R: buffer::Cursor + 'input>(
        input: &scope::Deferred<'input, 'd, R>,
    ) -> Result<Self, failure::Error> {
        let ((msgs, users), queue) =
            fields::Decoder::<Declaration>::hold(input, Declaration::QUEUE, |decoder| {
                Ok((decoder.read::<u32>()?, decoder.read::<u32>()?))
            })?;
        Ok(Self {
            queue,
            message_count: msgs,
            consumer_count: users,
        })
    }

    pub(crate) fn try_queue(&self) -> Result<short::Name<'input>, failure::Error> {
        if self.queue.is_empty() {
            return Err(failure::Error::Protocol(Fault::QueueNameEmpty));
        }
        short::Name::new(self.queue, short::Resource::BrokerQueue)
    }

    pub(crate) fn retain<'b, 'd>(self, queue: resident::Short<'b, 'd>) -> DeclareOk<'b, 'd> {
        DeclareOk {
            queue,
            message_count: self.message_count,
            consumer_count: self.consumer_count,
        }
    }

    pub(crate) fn try_name<'b, 'd>(
        &self,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<resident::Short<'b, 'd>, failure::Error> {
        self.queue
            .try_copy(Allocation::BrokerQueue, budget)
            .map(Into::into)
    }
}

pub struct Bind<'a> {
    pub queue: short::Name<'a>,
    pub exchange: short::Name<'a>,
    pub routing_key: short::Short<'a>,
    pub arguments: &'a field::Table,
}

impl method::Spec for Bind<'_> {
    const ID: method::Id = BIND;
}

impl method::Args for Bind<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_name(self.queue);
        out.put_name(self.exchange);
        out.put_short(self.routing_key);
        out.put_bits([false]);
        self.arguments.encode(out);
    }
}

impl method::Record<Binding> for Bind<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, Binding>,
    ) -> outputs::Fields<'a, Binding, schema::Identity<3>> {
        out.put_u16(0);
        let mut out = out
            .put_field(Binding::QUEUE, self.queue.into())
            .put_field(Binding::EXCHANGE, self.exchange.into())
            .put_field(Binding::ROUTING_KEY, self.routing_key);
        out.put_bits([false]);
        out.put_identity(self.arguments)
    }
}

impl method::Request for Bind<'_> {
    type Response = BindOk;
}

impl<'d> method::Op<'d> for Bind<'_> {
    type Output<'b, Consumer> = ();
}

pub struct BindOk;

impl method::Spec for BindOk {
    const ID: method::Id = BIND_OK;
}

impl method::Unit for BindOk {
    const KIND: method::Kind = method::Kind::QueueBind;
}

pub struct Unbind<'a> {
    pub queue: short::Name<'a>,
    pub exchange: short::Name<'a>,
    pub routing_key: short::Short<'a>,
    pub arguments: &'a field::Table,
}

impl method::Spec for Unbind<'_> {
    const ID: method::Id = UNBIND;
}

impl method::Args for Unbind<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_name(self.queue);
        out.put_name(self.exchange);
        out.put_short(self.routing_key);
        self.arguments.encode(out);
    }
}

impl method::Record<Binding> for Unbind<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, Binding>,
    ) -> outputs::Fields<'a, Binding, schema::Identity<3>> {
        out.put_u16(0);
        out.put_field(Binding::QUEUE, self.queue.into())
            .put_field(Binding::EXCHANGE, self.exchange.into())
            .put_field(Binding::ROUTING_KEY, self.routing_key)
            .put_identity(self.arguments)
    }
}

impl method::Request for Unbind<'_> {
    type Response = UnbindOk;
}

impl<'d> method::Op<'d> for Unbind<'_> {
    type Output<'b, Consumer> = ();
}

pub struct UnbindOk;

impl method::Spec for UnbindOk {
    const ID: method::Id = UNBIND_OK;
}

impl method::Unit for UnbindOk {
    const KIND: method::Kind = method::Kind::QueueUnbind;
}

pub struct Purge<'a> {
    pub queue: short::Name<'a>,
}

impl method::Spec for Purge<'_> {
    const ID: method::Id = PURGE;
}

impl method::Args for Purge<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_name(self.queue);
        out.put_bits([false]);
    }
}

impl method::Request for Purge<'_> {
    type Response = PurgeOk;
}

impl<'d> method::Op<'d> for Purge<'_> {
    type Output<'b, Consumer> = PurgeOk;
}

pub struct PurgeOk {
    pub message_count: u32,
}

impl method::Spec for PurgeOk {
    const ID: method::Id = PURGE_OK;
}

impl method::Reply for PurgeOk {
    const KIND: method::Kind = method::Kind::QueuePurge;
    type Output<'b, 'd, Consumer> = Self;
    type Value<'b, 'd, Consumer> = Self;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Self::Output<'b, 'd, Consumer>, failure::Error> {
        let actual = reply.kind();
        match reply {
            protocol::Response::QueuePurgeOk(ok) => Ok(ok),
            _ => Err(failure::Error::Protocol(Fault::Response {
                expected: method::Kind::QueuePurge,
                actual,
            })),
        }
    }
}

impl PurgeOk {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        Ok(Self {
            message_count: reader.read::<u32>()?,
        })
    }
}

pub struct Delete<'a> {
    pub queue: short::Name<'a>,
    pub if_unused: bool,
    pub if_empty: bool,
}

impl method::Spec for Delete<'_> {
    const ID: method::Id = DELETE;
}

impl method::Args for Delete<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_name(self.queue);
        out.put_bits([self.if_unused, self.if_empty, false]);
    }
}

impl method::Record<resource::Layout> for Delete<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, resource::Layout>,
    ) -> outputs::Fields<'a, resource::Layout, [schema::Offset; 1]> {
        out.put_u16(0);
        let mut out = out.put_field(resource::Layout::NAME, self.queue.into());
        out.put_bits([self.if_unused, self.if_empty, false]);
        out
    }
}

impl method::Request for Delete<'_> {
    type Response = DeleteOk;
}

impl<'d> method::Op<'d> for Delete<'_> {
    type Output<'b, Consumer> = DeleteOk;
}

pub struct DeleteOk {
    pub message_count: u32,
}

impl method::Spec for DeleteOk {
    const ID: method::Id = DELETE_OK;
}

impl method::Reply for DeleteOk {
    const KIND: method::Kind = method::Kind::QueueDelete;
    type Output<'b, 'd, Consumer> = Self;
    type Value<'b, 'd, Consumer> = Self;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Self::Output<'b, 'd, Consumer>, failure::Error> {
        let actual = reply.kind();
        match reply {
            protocol::Response::QueueDeleteOk(ok) => Ok(ok),
            _ => Err(failure::Error::Protocol(Fault::Response {
                expected: method::Kind::QueueDelete,
                actual,
            })),
        }
    }
}

impl DeleteOk {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        Ok(Self {
            message_count: reader.read::<u32>()?,
        })
    }
}
