use super::encode::ClientMethod;
use super::spec::{Channel, ClassId, Method};
use crate::Error;
use crate::wire::{Put, Reader, Sink};

pub struct Open;

impl Method for Open {
    const CLASS_ID: u16 = ClassId::CHANNEL;
    const METHOD_ID: u16 = Channel::OPEN;
}

impl ClientMethod for Open {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_short_str("");
    }
}

#[derive(Debug)]
pub struct OpenOk;

impl Method for OpenOk {
    const CLASS_ID: u16 = ClassId::CHANNEL;
    const METHOD_ID: u16 = Channel::OPEN_OK;
}

impl OpenOk {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let _reserved = r.take_long_str()?;
        Ok(Self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Flow {
    pub active: bool,
}

impl Method for Flow {
    const CLASS_ID: u16 = ClassId::CHANNEL;
    const METHOD_ID: u16 = Channel::FLOW;
}

impl Flow {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            active: r.take_u8()? & 1 == 1,
        })
    }
}

impl ClientMethod for Flow {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_bool(self.active);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FlowOk {
    pub active: bool,
}

impl Method for FlowOk {
    const CLASS_ID: u16 = ClassId::CHANNEL;
    const METHOD_ID: u16 = Channel::FLOW_OK;
}

impl FlowOk {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            active: r.take_u8()? & 1 == 1,
        })
    }
}

impl ClientMethod for FlowOk {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_bool(self.active);
    }
}

#[derive(Debug, Clone)]
pub struct Close {
    pub reply_code: u16,
    pub reply_text: String,
    pub class_id: u16,
    pub method_id: u16,
}

impl Method for Close {
    const CLASS_ID: u16 = ClassId::CHANNEL;
    const METHOD_ID: u16 = Channel::CLOSE;
}

impl Close {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            reply_code: r.take_u16()?,
            reply_text: r.take_short_str()?.to_string(),
            class_id: r.take_u16()?,
            method_id: r.take_u16()?,
        })
    }
}

impl ClientMethod for Close {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(self.reply_code);
        out.put_short_str(&self.reply_text);
        out.put_u16(self.class_id);
        out.put_u16(self.method_id);
    }
}

#[derive(Debug)]
pub struct CloseOk;

impl Method for CloseOk {
    const CLASS_ID: u16 = ClassId::CHANNEL;
    const METHOD_ID: u16 = Channel::CLOSE_OK;
}

impl CloseOk {
    pub fn read(_r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

impl ClientMethod for CloseOk {
    fn write_args<S: Sink>(&self, _out: &mut S) {}
}
