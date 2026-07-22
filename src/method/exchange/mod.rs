use super::core::{ClassId, ClientMethod, Exchange, Method};
use crate::error::Error;
use crate::wire::field::FieldTable;
use crate::wire::put::Put;
use crate::wire::reader::Reader;
use crate::wire::sink::Sink;

pub struct Declare<'a> {
    pub exchange: &'a str,
    pub kind: &'a str,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub no_wait: bool,
    pub arguments: &'a FieldTable,
}

impl Method for Declare<'_> {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::DECLARE;
}

impl ClientMethod for Declare<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.exchange);
        out.put_short_str(self.kind);
        out.put_bits(&[
            self.passive,
            self.durable,
            self.auto_delete,
            self.internal,
            self.no_wait,
        ]);
        self.arguments.encode(out);
    }
}

#[derive(Debug)]
pub struct DeclareOk;

impl Method for DeclareOk {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::DECLARE_OK;
}

impl DeclareOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Delete<'a> {
    pub exchange: &'a str,
    pub if_unused: bool,
    pub no_wait: bool,
}

impl Method for Delete<'_> {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::DELETE;
}

impl ClientMethod for Delete<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.exchange);
        out.put_bits(&[self.if_unused, self.no_wait]);
    }
}

#[derive(Debug)]
pub struct DeleteOk;

impl Method for DeleteOk {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::DELETE_OK;
}

impl DeleteOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Bind<'a> {
    pub destination: &'a str,
    pub source: &'a str,
    pub routing_key: &'a str,
    pub no_wait: bool,
    pub arguments: &'a FieldTable,
}

impl Method for Bind<'_> {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::BIND;
}

impl ClientMethod for Bind<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.destination);
        out.put_short_str(self.source);
        out.put_short_str(self.routing_key);
        out.put_bits(&[self.no_wait]);
        self.arguments.encode(out);
    }
}

#[derive(Debug)]
pub struct BindOk;

impl Method for BindOk {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::BIND_OK;
}

impl BindOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Unbind<'a> {
    pub destination: &'a str,
    pub source: &'a str,
    pub routing_key: &'a str,
    pub no_wait: bool,
    pub arguments: &'a FieldTable,
}

impl Method for Unbind<'_> {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::UNBIND;
}

impl ClientMethod for Unbind<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_short_str(self.destination);
        out.put_short_str(self.source);
        out.put_short_str(self.routing_key);
        out.put_bits(&[self.no_wait]);
        self.arguments.encode(out);
    }
}

#[derive(Debug)]
pub struct UnbindOk;

impl Method for UnbindOk {
    const CLASS_ID: u16 = ClassId::EXCHANGE;
    const METHOD_ID: u16 = Exchange::UNBIND_OK;
}

impl UnbindOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}
