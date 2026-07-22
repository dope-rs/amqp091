use super::core::{ClassId, ClientMethod, Method, Queue};
use crate::error::Error;
use crate::wire::field::FieldTable;
use crate::wire::put::Put;
use crate::wire::reader::Reader;
use crate::wire::sink::Sink;

pub struct Declare<'a> {
    pub queue: &'a str,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub no_wait: bool,
    pub arguments: &'a FieldTable,
}

impl Method for Declare<'_> {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::DECLARE;
}

impl ClientMethod for Declare<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_bits(&[
            self.passive,
            self.durable,
            self.exclusive,
            self.auto_delete,
            self.no_wait,
        ]);
        self.arguments.encode(out);
    }
}

#[derive(Debug)]
pub struct DeclareOk {
    pub queue: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

impl Method for DeclareOk {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::DECLARE_OK;
}

impl DeclareOk {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            queue: r.take_short_str()?.to_string(),
            message_count: r.take_u32()?,
            consumer_count: r.take_u32()?,
        })
    }
}

pub struct Bind<'a> {
    pub queue: &'a str,
    pub exchange: &'a str,
    pub routing_key: &'a str,
    pub no_wait: bool,
    pub arguments: &'a FieldTable,
}

impl Method for Bind<'_> {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::BIND;
}

impl ClientMethod for Bind<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_short_str(self.exchange);
        out.put_short_str(self.routing_key);
        out.put_bits(&[self.no_wait]);
        self.arguments.encode(out);
    }
}

#[derive(Debug)]
pub struct BindOk;

impl Method for BindOk {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::BIND_OK;
}

impl BindOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Unbind<'a> {
    pub queue: &'a str,
    pub exchange: &'a str,
    pub routing_key: &'a str,
    pub arguments: &'a FieldTable,
}

impl Method for Unbind<'_> {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::UNBIND;
}

impl ClientMethod for Unbind<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_short_str(self.exchange);
        out.put_short_str(self.routing_key);
        self.arguments.encode(out);
    }
}

#[derive(Debug)]
pub struct UnbindOk;

impl Method for UnbindOk {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::UNBIND_OK;
}

impl UnbindOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Purge<'a> {
    pub queue: &'a str,
    pub no_wait: bool,
}

impl Method for Purge<'_> {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::PURGE;
}

impl ClientMethod for Purge<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_bits(&[self.no_wait]);
    }
}

#[derive(Debug)]
pub struct PurgeOk {
    pub message_count: u32,
}

impl Method for PurgeOk {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::PURGE_OK;
}

impl PurgeOk {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            message_count: r.take_u32()?,
        })
    }
}

pub struct Delete<'a> {
    pub queue: &'a str,
    pub if_unused: bool,
    pub if_empty: bool,
    pub no_wait: bool,
}

impl Method for Delete<'_> {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::DELETE;
}

impl ClientMethod for Delete<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.queue);
        out.put_bits(&[self.if_unused, self.if_empty, self.no_wait]);
    }
}

#[derive(Debug)]
pub struct DeleteOk {
    pub message_count: u32,
}

impl Method for DeleteOk {
    const CLASS_ID: u16 = ClassId::QUEUE;
    const METHOD_ID: u16 = Queue::DELETE_OK;
}

impl DeleteOk {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            message_count: r.take_u32()?,
        })
    }
}
