use o3::buffer::Shared;

use super::core::{Basic, ClassId, ClientMethod, Method};
use crate::error::Error;
use crate::wire::field::FieldTable;
use crate::wire::put::Put;
use crate::wire::reader::Reader;
use crate::wire::sink::Sink;
use crate::wire::text::SharedStr;

pub struct Qos {
    pub prefetch_size: u32,
    pub prefetch_count: u16,
    pub global: bool,
}

impl Method for Qos {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::QOS;
}

impl ClientMethod for Qos {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u32(self.prefetch_size);
        out.put_u16(self.prefetch_count);
        out.put_bits(&[self.global]);
    }
}

#[derive(Debug)]
pub struct QosOk;

impl Method for QosOk {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::QOS_OK;
}

impl QosOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Consume<'a> {
    pub queue: &'a str,
    pub consumer_tag: &'a str,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub no_wait: bool,
    pub arguments: &'a FieldTable,
}

impl Method for Consume<'_> {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::CONSUME;
}

impl ClientMethod for Consume<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_short_str(self.consumer_tag);
        out.put_bits(&[self.no_local, self.no_ack, self.exclusive, self.no_wait]);
        self.arguments.encode(out);
    }
}

#[derive(Debug, Clone)]
pub struct ConsumeOk {
    pub consumer_tag: String,
}

impl Method for ConsumeOk {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::CONSUME_OK;
}

impl ConsumeOk {
    pub fn read(reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            consumer_tag: reader.take_short_str()?.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Cancel {
    pub consumer_tag: String,
    pub no_wait: bool,
}

impl Method for Cancel {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::CANCEL;
}

impl Cancel {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let consumer_tag = r.take_short_str()?.to_string();
        let flags = r.take_u8()?;
        Ok(Self {
            consumer_tag,
            no_wait: flags & 1 == 1,
        })
    }
}

impl ClientMethod for Cancel {
    fn write_args<S: Sink>(&self, out: &mut S) {
        write_cancel(out, &self.consumer_tag, self.no_wait);
    }
}

pub struct CancelRef<'a> {
    pub consumer_tag: &'a str,
    pub no_wait: bool,
}

impl Method for CancelRef<'_> {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::CANCEL;
}

impl ClientMethod for CancelRef<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        write_cancel(out, self.consumer_tag, self.no_wait);
    }
}

fn write_cancel<S: Sink>(out: &mut S, consumer_tag: &str, no_wait: bool) {
    out.put_short_str(consumer_tag);
    out.put_bits(&[no_wait]);
}

#[derive(Debug, Clone)]
pub struct CancelOk {
    pub consumer_tag: String,
}

impl Method for CancelOk {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::CANCEL_OK;
}

impl CancelOk {
    pub fn read(reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            consumer_tag: reader.take_short_str()?.to_string(),
        })
    }
}

impl ClientMethod for CancelOk {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_short_str(&self.consumer_tag);
    }
}

pub struct Publish<'a> {
    pub exchange: &'a str,
    pub routing_key: &'a str,
    pub mandatory: bool,
    pub immediate: bool,
}

impl Method for Publish<'_> {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::PUBLISH;
}

impl ClientMethod for Publish<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.exchange);
        out.put_short_str(self.routing_key);
        out.put_bits(&[self.mandatory, self.immediate]);
    }
}

#[derive(Debug, Clone)]
pub struct Return {
    pub reply_code: u16,
    pub reply_text: SharedStr,
    pub exchange: SharedStr,
    pub routing_key: SharedStr,
}

impl Method for Return {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::RETURN;
}

impl Return {
    pub fn read(r: &mut Reader<'_>, payload: &Shared) -> Result<Self, Error> {
        let reply_code = r.take_u16()?;
        let reply_text = r.take_short_str_range(payload)?;
        let exchange = r.take_short_str_range(payload)?;
        let routing_key = r.take_short_str_range(payload)?;
        Ok(Self {
            reply_code,
            reply_text: SharedStr::new(payload.slice(reply_text)),
            exchange: SharedStr::new(payload.slice(exchange)),
            routing_key: SharedStr::new(payload.slice(routing_key)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Deliver {
    pub consumer_tag: SharedStr,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: SharedStr,
    pub routing_key: SharedStr,
}

impl Method for Deliver {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::DELIVER;
}

impl Deliver {
    pub fn read(r: &mut Reader<'_>, payload: &Shared) -> Result<Self, Error> {
        let consumer_tag = r.take_short_str_range(payload)?;
        let delivery_tag = r.take_u64()?;
        let redelivered = r.take_u8()? & 1 == 1;
        let exchange = r.take_short_str_range(payload)?;
        let routing_key = r.take_short_str_range(payload)?;
        Ok(Self {
            consumer_tag: SharedStr::new(payload.slice(consumer_tag)),
            delivery_tag,
            redelivered,
            exchange: SharedStr::new(payload.slice(exchange)),
            routing_key: SharedStr::new(payload.slice(routing_key)),
        })
    }
}

pub struct Get<'a> {
    pub queue: &'a str,
    pub no_ack: bool,
}

impl Method for Get<'_> {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::GET;
}

impl ClientMethod for Get<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_bits(&[self.no_ack]);
    }
}

#[derive(Debug, Clone)]
pub struct GetOk {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: SharedStr,
    pub routing_key: SharedStr,
    pub message_count: u32,
}

impl Method for GetOk {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::GET_OK;
}

impl GetOk {
    pub fn read(r: &mut Reader<'_>, payload: &Shared) -> Result<Self, Error> {
        let delivery_tag = r.take_u64()?;
        let redelivered = r.take_u8()? & 1 == 1;
        let exchange = r.take_short_str_range(payload)?;
        let routing_key = r.take_short_str_range(payload)?;
        let message_count = r.take_u32()?;
        Ok(Self {
            delivery_tag,
            redelivered,
            exchange: SharedStr::new(payload.slice(exchange)),
            routing_key: SharedStr::new(payload.slice(routing_key)),
            message_count,
        })
    }
}

#[derive(Debug)]
pub struct GetEmpty;

impl Method for GetEmpty {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::GET_EMPTY;
}

impl GetEmpty {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let _reserved = r.take_short_str()?;
        Ok(Self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Ack {
    pub delivery_tag: u64,
    pub multiple: bool,
}

impl Method for Ack {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::ACK;
}

impl Ack {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            delivery_tag: r.take_u64()?,
            multiple: r.take_u8()? & 1 == 1,
        })
    }
}

impl ClientMethod for Ack {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u64(self.delivery_tag);
        out.put_bits(&[self.multiple]);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Reject {
    pub delivery_tag: u64,
    pub requeue: bool,
}

impl Method for Reject {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::REJECT;
}

impl ClientMethod for Reject {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u64(self.delivery_tag);
        out.put_bits(&[self.requeue]);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Nack {
    pub delivery_tag: u64,
    pub multiple: bool,
    pub requeue: bool,
}

impl Method for Nack {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::NACK;
}

impl Nack {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let delivery_tag = r.take_u64()?;
        let flags = r.take_u8()?;
        Ok(Self {
            delivery_tag,
            multiple: flags & 1 == 1,
            requeue: flags & 2 == 2,
        })
    }
}

impl ClientMethod for Nack {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u64(self.delivery_tag);
        out.put_bits(&[self.multiple, self.requeue]);
    }
}

pub struct Recover {
    pub requeue: bool,
}

impl Method for Recover {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::RECOVER;
}

impl ClientMethod for Recover {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_bits(&[self.requeue]);
    }
}

#[derive(Debug)]
pub struct RecoverOk;

impl Method for RecoverOk {
    const CLASS_ID: u16 = ClassId::BASIC;
    const METHOD_ID: u16 = Basic::RECOVER_OK;
}

impl RecoverOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}
