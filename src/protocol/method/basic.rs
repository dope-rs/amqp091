use std::num;

use o3::buffer::hold::scope;
use o3::buffer::{self};

use crate::failure;
use crate::failure::wire::{Allocation, Fault};
use crate::message::{ack, content, field};
use crate::protocol::wire::schema::{self, consume};
use crate::protocol::wire::text::{fields, resident, short};
use crate::protocol::wire::{Put as _, outputs, reader};
use crate::protocol::{self, class, method, wire};

pub(super) const QOS: method::Id = method::Id::new(class::BASIC, 10);
pub(super) const QOS_OK: method::Id = method::Id::new(class::BASIC, 11);
pub(super) const CONSUME: method::Id = method::Id::new(class::BASIC, 20);
pub(super) const CONSUME_OK: method::Id = method::Id::new(class::BASIC, 21);
pub(super) const CANCEL: method::Id = method::Id::new(class::BASIC, 30);
pub(super) const CANCEL_OK: method::Id = method::Id::new(class::BASIC, 31);
pub(super) const PUBLISH: method::Id = method::Id::new(class::BASIC, 40);
pub(super) const RETURN: method::Id = method::Id::new(class::BASIC, 50);
pub(super) const DELIVER: method::Id = method::Id::new(class::BASIC, 60);
pub(super) const GET_OK: method::Id = method::Id::new(class::BASIC, 71);
pub(super) const GET_EMPTY: method::Id = method::Id::new(class::BASIC, 72);
pub(super) const ACK: method::Id = method::Id::new(class::BASIC, 80);
pub(super) const REJECT: method::Id = method::Id::new(class::BASIC, 90);
pub(super) const RECOVER: method::Id = method::Id::new(class::BASIC, 110);
pub(super) const RECOVER_OK: method::Id = method::Id::new(class::BASIC, 111);
pub(super) const NACK: method::Id = method::Id::new(class::BASIC, 120);

enum ConsumerTag {}
const CONSUMER_TAG: schema::Field<ConsumerTag, 0> = schema::Field::new();

pub struct Qos {
    pub prefetch_size: u32,
    pub prefetch_count: u16,
    pub global: bool,
}

impl method::Spec for Qos {
    const ID: method::Id = QOS;
}

impl method::Args for Qos {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u32(self.prefetch_size);
        out.put_u16(self.prefetch_count);
        out.put_bits([self.global]);
    }
}

impl method::Request for Qos {
    type Response = QosOk;
}

impl<'d> method::Op<'d> for Qos {
    type Output<'b, Consumer> = ();
}

pub struct QosOk;

impl method::Spec for QosOk {
    const ID: method::Id = QOS_OK;
}

impl method::Unit for QosOk {
    const KIND: method::Kind = method::Kind::Qos;
}

pub struct Consume<'a, M = ack::Manual> {
    pub queue: short::Name<'a>,
    pub consumer_tag: short::Short<'a>,
    pub no_local: bool,
    pub ack: M,
    pub exclusive: bool,
    pub arguments: &'a field::Table,
}

impl<M: ack::Mode> method::Spec for Consume<'_, M> {
    const ID: method::Id = CONSUME;
}

impl<M: ack::Mode> method::Args for Consume<'_, M> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_name(self.queue);
        out.put_short(self.consumer_tag);
        out.put_bits([self.no_local, M::NO_ACK, self.exclusive, false]);
        self.arguments.encode(out);
    }
}

impl<M: ack::Mode> method::Record<consume::Layout> for Consume<'_, M> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, consume::Layout>,
    ) -> outputs::Fields<'a, consume::Layout, [schema::Offset; 2]> {
        out.put_u16(0);
        let mut out = out
            .put_field(consume::Layout::QUEUE, self.queue.into())
            .put_field(consume::Layout::CONSUMER_TAG, self.consumer_tag);
        out.put_bits([self.no_local, M::NO_ACK, self.exclusive, false]);
        out.put_table(self.arguments);
        out
    }
}

impl<M: ack::Mode> method::Request for Consume<'_, M> {
    type Response = ConsumeReply;
}

impl<'d, M: ack::Mode> method::Op<'d> for Consume<'_, M> {
    type Output<'b, Consumer> = protocol::Consumer<'b, 'd, Consumer, M>;
}

pub struct ConsumeOk<'b, 'd> {
    pub consumer_tag: resident::Short<'b, 'd>,
}

pub(crate) struct View<'input> {
    tag: short::Short<'input>,
}

impl method::Spec for ConsumeOk<'_, '_> {
    const ID: method::Id = CONSUME_OK;
}

#[doc(hidden)]
pub enum ConsumeReply {}

impl method::Spec for ConsumeReply {
    const ID: method::Id = CONSUME_OK;
}

impl method::Reply for ConsumeReply {
    const KIND: method::Kind = method::Kind::Consume;
    type Output<'b, 'd, Consumer> = protocol::Registration<'b, 'd, Consumer>;
    type Value<'b, 'd, Consumer> = protocol::Registration<'b, 'd, Consumer>;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Self::Output<'b, 'd, Consumer>, failure::Error> {
        match reply {
            protocol::Response::BasicConsumeOk(ok, key) => {
                Ok(protocol::Registration::Registered(ok, key))
            }
            protocol::Response::BasicConsumeReplayOk => Ok(protocol::Registration::Replayed),
            reply => Err(failure::Error::Protocol(Fault::Response {
                expected: method::Kind::Consume,
                actual: reply.kind(),
            })),
        }
    }
}

impl<'input> View<'input> {
    pub(crate) fn read<'d, R: buffer::Cursor + 'input>(
        input: &scope::Deferred<'input, 'd, R>,
    ) -> Result<Self, failure::Error> {
        fields::Decoder::<ConsumerTag>::decode(input, CONSUMER_TAG).map(|tag| Self { tag })
    }

    pub(crate) const fn tag(&self) -> short::Short<'input> {
        self.tag
    }

    pub(crate) fn try_retain<'b, 'd>(
        &self,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<resident::Short<'b, 'd>, failure::Error> {
        Ok(self
            .tag
            .try_copy(Allocation::BrokerConsumer, budget)?
            .into())
    }
}

#[derive(Clone)]
pub struct Cancel<'a> {
    consumer_tag: short::Short<'a>,
    pub no_wait: bool,
}

impl method::Spec for Cancel<'_> {
    const ID: method::Id = CANCEL;
}

impl<'a> Cancel<'a> {
    pub(crate) fn read(reader: &mut reader::Reader<'a>) -> Result<Self, fields::Decode> {
        let tag = short::Short::new(reader.short()?)?;
        let flags = reader.read::<u8>()?;
        Ok(Self {
            consumer_tag: tag,
            no_wait: flags & 1 == 1,
        })
    }

    pub fn consumer_tag(&self) -> short::Short<'_> {
        self.consumer_tag
    }
}

impl method::Args for Cancel<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_cancel(out, self.consumer_tag(), self.no_wait);
    }
}

impl method::Request for Cancel<'_> {
    type Response = CancelReply;
}

pub struct CancelRef<'a> {
    pub consumer_tag: short::Short<'a>,
}

impl method::Spec for CancelRef<'_> {
    const ID: method::Id = CANCEL;
}

impl method::Args for CancelRef<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_cancel(out, self.consumer_tag, false);
    }
}

impl method::Request for CancelRef<'_> {
    type Response = CancelReply;
}

impl<'d> method::Op<'d> for CancelRef<'_> {
    type Output<'b, Consumer> = CancelOk<'d>;
}

fn write_cancel<S: wire::Put>(out: &mut S, tag: short::Short<'_>, wait: bool) {
    out.put_short(tag);
    out.put_bits([wait]);
}

pub struct CancelOk<'d> {
    pub consumer_tag: resident::Exact<'d>,
}

impl method::Spec for CancelOk<'_> {
    const ID: method::Id = CANCEL_OK;
}

#[doc(hidden)]
pub enum CancelReply {}

impl method::Spec for CancelReply {
    const ID: method::Id = CANCEL_OK;
}

impl method::Reply for CancelReply {
    const KIND: method::Kind = method::Kind::CancelOk;
    type Output<'b, 'd, Consumer> = CancelOk<'d>;
    type Value<'b, 'd, Consumer> = CancelOk<'d>;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Self::Output<'b, 'd, Consumer>, failure::Error> {
        let actual = reply.kind();
        match reply {
            protocol::Response::BasicCancelOk(ok) => Ok(ok),
            _ => Err(failure::Error::Protocol(Fault::Response {
                expected: method::Kind::CancelOk,
                actual,
            })),
        }
    }
}

impl<'d> CancelOk<'d> {
    pub(crate) fn read<'input, R: buffer::Cursor + 'input>(
        input: &scope::Deferred<'input, 'd, R>,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<Self, failure::Error> {
        let ((), text) = fields::Decoder::<ConsumerTag>::retain(input, budget, |dec| {
            let (text, dec) = dec.short(CONSUMER_TAG)?;
            Ok((((), text), dec))
        })?;
        Ok(Self { consumer_tag: text })
    }
}

impl method::Args for CancelOk<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_short(short::Short::from(&self.consumer_tag));
    }
}

pub struct Publish<'a> {
    pub exchange: short::Short<'a>,
    pub routing_key: short::Short<'a>,
    pub mandatory: bool,
    pub immediate: bool,
}

impl method::Spec for Publish<'_> {
    const ID: method::Id = PUBLISH;
}

impl method::Args for Publish<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short(self.exchange);
        out.put_short(self.routing_key);
        out.put_bits([self.mandatory, self.immediate]);
    }
}

pub struct Return<'a> {
    pub reply_code: u16,
    pub(crate) text: fields::View<'a, content::ReturnText, 3>,
}

impl method::Spec for Return<'_> {
    const ID: method::Id = RETURN;
}

impl<'input> Return<'input> {
    pub(crate) fn read<'d, R: buffer::Cursor + 'input>(
        input: &scope::Deferred<'input, 'd, R>,
    ) -> Result<Self, failure::Error> {
        let (code, text) = fields::View::<content::ReturnText, 3>::decode(input, |mut dec| {
            let code = dec.read::<u16>()?;
            let (_, dec) = dec.short(content::ReturnText::REPLY)?;
            let (_, dec) = dec.short(content::ReturnText::EXCHANGE)?;
            let (_, dec) = dec.short(content::ReturnText::ROUTING_KEY)?;
            Ok((code, dec))
        })?;
        Ok(Self {
            reply_code: code,
            text,
        })
    }

    pub fn reply_text(&self) -> &'input str {
        self.text.get(content::ReturnText::REPLY)
    }

    pub fn exchange(&self) -> &'input str {
        self.text.get(content::ReturnText::EXCHANGE)
    }

    pub fn routing_key(&self) -> &'input str {
        self.text.get(content::ReturnText::ROUTING_KEY)
    }
}

pub struct Deliver<'a> {
    pub delivery_tag: num::NonZero<u64>,
    pub redelivered: bool,
    pub(crate) text: fields::View<'a, content::DeliveryText, 3>,
}

impl method::Spec for Deliver<'_> {
    const ID: method::Id = DELIVER;
}

impl<'input> Deliver<'input> {
    pub(crate) fn read<'d, R: buffer::Cursor + 'input>(
        input: &scope::Deferred<'input, 'd, R>,
    ) -> Result<Self, failure::Error> {
        let ((tag, again), text) =
            fields::View::<content::DeliveryText, 3>::decode(input, |dec| {
                let (_, mut dec) = dec.short(content::DeliveryText::CONSUMER_TAG)?;
                let tag = dec.read::<u64>()?;
                let again = dec.read::<u8>()? & 1 == 1;
                let (_, dec) = dec.short(content::DeliveryText::EXCHANGE)?;
                let (_, dec) = dec.short(content::DeliveryText::ROUTING_KEY)?;
                Ok(((tag, again), dec))
            })?;
        Ok(Self {
            delivery_tag: num::NonZero::new(tag)
                .ok_or(failure::Error::Protocol(Fault::DeliveryTagZero))?,
            redelivered: again,
            text,
        })
    }

    pub fn consumer_tag(&self) -> &'input str {
        self.text.get(content::DeliveryText::CONSUMER_TAG)
    }

    pub fn exchange(&self) -> &'input str {
        self.text.get(content::DeliveryText::EXCHANGE)
    }

    pub fn routing_key(&self) -> &'input str {
        self.text.get(content::DeliveryText::ROUTING_KEY)
    }
}

#[derive(Clone, Copy)]
pub struct Ack {
    pub delivery_tag: u64,
    pub multiple: bool,
}

impl method::Spec for Ack {
    const ID: method::Id = ACK;
}

impl Ack {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        Ok(Self {
            delivery_tag: reader.read::<u64>()?,
            multiple: reader.read::<u8>()? & 1 == 1,
        })
    }
}

impl method::Args for Ack {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u64(self.delivery_tag);
        out.put_bits([self.multiple]);
    }
}

#[derive(Clone, Copy)]
pub struct Reject {
    pub delivery_tag: num::NonZero<u64>,
    pub requeue: bool,
}

impl method::Spec for Reject {
    const ID: method::Id = REJECT;
}

impl method::Args for Reject {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u64(self.delivery_tag.get());
        out.put_bits([self.requeue]);
    }
}

#[derive(Clone, Copy)]
pub struct Nack {
    pub delivery_tag: u64,
    pub multiple: bool,
    pub requeue: bool,
}

impl method::Spec for Nack {
    const ID: method::Id = NACK;
}

impl Nack {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        let tag = reader.read::<u64>()?;
        let flags = reader.read::<u8>()?;
        Ok(Self {
            delivery_tag: tag,
            multiple: flags & 1 == 1,
            requeue: flags & 2 == 2,
        })
    }
}

impl method::Args for Nack {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u64(self.delivery_tag);
        out.put_bits([self.multiple, self.requeue]);
    }
}

pub struct Recover {
    pub requeue: bool,
}

impl method::Spec for Recover {
    const ID: method::Id = RECOVER;
}

impl method::Args for Recover {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_bits([self.requeue]);
    }
}

impl method::Request for Recover {
    type Response = RecoverOk;
}

impl<'d> method::Op<'d> for Recover {
    type Output<'b, Consumer> = ();
}

pub struct RecoverOk;

impl method::Spec for RecoverOk {
    const ID: method::Id = RECOVER_OK;
}

impl method::Unit for RecoverOk {
    const KIND: method::Kind = method::Kind::Recover;
}
