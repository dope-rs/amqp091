use o3::buffer::hold::scope;
use o3::buffer::{self};

use crate::failure;
use crate::failure::wire::Fault;
use crate::protocol::method::Spec as _;
use crate::protocol::wire::text::{fields, resident, short};
use crate::protocol::wire::{Write, frame, reader, schema};
use crate::protocol::{class, method, wire};

mod event;

pub(crate) use event::Event;

pub(super) const OPEN: method::Id = method::Id::new(class::CHANNEL, 10);
pub(super) const OPEN_OK: method::Id = method::Id::new(class::CHANNEL, 11);
pub(super) const FLOW: method::Id = method::Id::new(class::CHANNEL, 20);
pub(super) const FLOW_OK: method::Id = method::Id::new(class::CHANNEL, 21);
pub(super) const CLOSE: method::Id = method::Id::new(class::CHANNEL, 40);
pub(super) const CLOSE_OK: method::Id = method::Id::new(class::CHANNEL, 41);

enum CloseText {}

impl CloseText {
    const REPLY: schema::Field<Self, 0> = schema::Field::new();
}

pub struct Open;

impl method::Spec for Open {
    const ID: method::Id = OPEN;
}

impl method::Args for Open {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_short(short::Short::EMPTY);
    }
}

impl method::Request for Open {
    type Response = OpenOk;
}

impl<'d> method::Op<'d> for Open {
    type Output<'b, Consumer> = ();
}

pub struct OpenOk;

impl method::Spec for OpenOk {
    const ID: method::Id = OPEN_OK;
}

impl method::Unit for OpenOk {
    const KIND: method::Kind = method::Kind::Open;
}

impl OpenOk {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        let reserved = reader.long()?;
        if reserved.is_empty() {
            Ok(Self)
        } else {
            Err(failure::Error::Protocol(Fault::Reserved {
                method: OPEN_OK,
                actual: reserved.len(),
            })
            .into())
        }
    }
}

#[derive(Clone, Copy)]
pub struct Flow {
    pub active: bool,
}

impl method::Spec for Flow {
    const ID: method::Id = FLOW;
}

impl Flow {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        Ok(Self {
            active: reader.read::<u8>()? & 1 == 1,
        })
    }
}

impl method::Args for Flow {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.push(u8::from(self.active));
    }
}

#[derive(Clone, Copy)]
pub struct FlowOk {
    pub active: bool,
}

impl method::Spec for FlowOk {
    const ID: method::Id = FLOW_OK;
}

impl FlowOk {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        Ok(Self {
            active: reader.read::<u8>()? & 1 == 1,
        })
    }
}

impl method::Args for FlowOk {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.push(u8::from(self.active));
    }
}

pub struct Close<'d> {
    pub reply_code: u16,
    pub reply_text: resident::Exact<'d>,
    pub class_id: u16,
    pub method_id: u16,
}

impl method::Spec for Close<'_> {
    const ID: method::Id = CLOSE;
}

impl<'d> Close<'d> {
    pub(crate) fn retain<'input, R: buffer::Cursor + 'input>(
        payload: &scope::Deferred<'input, 'd, R>,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<Self, failure::Error> {
        let ((code, class, method), text) =
            fields::Decoder::<CloseText>::retain(payload, budget, |mut decoder| {
                let code = decoder.read::<u16>()?;
                let (text, mut decoder) = decoder.short(CloseText::REPLY)?;
                let class = decoder.read::<u16>()?;
                let method = decoder.read::<u16>()?;
                Ok((((code, class, method), text), decoder))
            })?;
        Ok(Self {
            reply_code: code,
            reply_text: text,
            class_id: class,
            method_id: method,
        })
    }
}

impl method::Args for Close<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(self.reply_code);
        out.put_short(short::Short::from(&self.reply_text));
        out.put_u16(self.class_id);
        out.put_u16(self.method_id);
    }
}

impl method::Request for Close<'_> {
    type Response = CloseOk;
}

impl<'d> method::Op<'d> for Close<'_> {
    type Output<'b, Consumer> = ();
}

#[doc(hidden)]
pub struct CloseRef<'a> {
    pub reply_code: u16,
    pub reply_text: short::Short<'a>,
    pub class_id: u16,
    pub method_id: u16,
}

impl method::Spec for CloseRef<'_> {
    const ID: method::Id = CLOSE;
}

impl method::Args for CloseRef<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(self.reply_code);
        out.put_short(self.reply_text);
        out.put_u16(self.class_id);
        out.put_u16(self.method_id);
    }
}

impl method::Request for CloseRef<'_> {
    type Response = CloseOk;
}

impl<'d> method::Op<'d> for CloseRef<'_> {
    type Output<'b, Consumer> = ();
}

pub struct CloseOk;

impl method::Spec for CloseOk {
    const ID: method::Id = CLOSE_OK;
}

impl method::Unit for CloseOk {
    const KIND: method::Kind = method::Kind::CloseOk;
}

impl method::Encode for CloseOk {
    fn write<S: Write + AsMut<[u8]> + ?Sized>(&self, out: &mut S, channel: u16) {
        frame::Encoder::new(out).method(channel, Self::ID.bytes());
    }
}
