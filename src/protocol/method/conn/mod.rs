use std::num::NonZero;

use o3::buffer::hold::scope;
use o3::buffer::{self};

use crate::failure;
use crate::failure::wire::Fault;
use crate::message::field;
use crate::protocol::method::Spec as _;
use crate::protocol::wire::text::{fields, long, resident, short};
use crate::protocol::wire::{Write, frame, reader, schema};
use crate::protocol::{class, method, wire};

mod event;
mod kind;

pub(crate) use event::Event;
pub(crate) use kind::Kind;

pub(super) const START: method::Id = method::Id::new(class::CONNECTION, 10);
pub(super) const START_OK: method::Id = method::Id::new(class::CONNECTION, 11);
pub(super) const SECURE: method::Id = method::Id::new(class::CONNECTION, 20);
pub(super) const SECURE_OK: method::Id = method::Id::new(class::CONNECTION, 21);
pub(super) const TUNE: method::Id = method::Id::new(class::CONNECTION, 30);
pub(super) const TUNE_OK: method::Id = method::Id::new(class::CONNECTION, 31);
pub(super) const OPEN: method::Id = method::Id::new(class::CONNECTION, 40);
pub(super) const OPEN_OK: method::Id = method::Id::new(class::CONNECTION, 41);
pub(super) const CLOSE: method::Id = method::Id::new(class::CONNECTION, 50);
pub(super) const CLOSE_OK: method::Id = method::Id::new(class::CONNECTION, 51);
pub(super) const BLOCKED: method::Id = method::Id::new(class::CONNECTION, 60);
pub(super) const UNBLOCKED: method::Id = method::Id::new(class::CONNECTION, 61);

enum CloseText {}

impl CloseText {
    const REPLY: schema::Field<Self, 0> = schema::Field::new();
}

enum BlockedText {}

impl BlockedText {
    const REASON: schema::Field<Self, 0> = schema::Field::new();
}

pub struct Start<'a> {
    pub version_major: u8,
    pub version_minor: u8,
    mechanisms: &'a str,
    locales: &'a str,
}

impl method::Spec for Start<'_> {
    const ID: method::Id = START;
}

impl<'a> Start<'a> {
    pub(crate) fn read(reader: &mut reader::Reader<'a>) -> Result<Self, fields::Decode> {
        let major = reader.read::<u8>()?;
        let minor = reader.read::<u8>()?;
        reader.long()?;
        let mechs = reader.long_str()?;
        let locales = reader.long_str()?;
        Ok(Self {
            version_major: major,
            version_minor: minor,
            mechanisms: mechs,
            locales,
        })
    }

    pub fn mechanisms(&self) -> &str {
        self.mechanisms
    }

    pub fn locales(&self) -> &str {
        self.locales
    }
}

pub struct StartOk<'a> {
    pub client_properties: &'a field::Table,
    pub mechanism: short::Short<'a>,
    pub response: long::Long<'a>,
    pub locale: short::Short<'a>,
}

impl method::Spec for StartOk<'_> {
    const ID: method::Id = START_OK;
}

impl method::Args for StartOk<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        self.client_properties.encode(out);
        out.put_short(self.mechanism);
        out.put_long(self.response);
        out.put_short(self.locale);
    }
}

pub struct Secure;

impl method::Spec for Secure {
    const ID: method::Id = SECURE;
}

impl Secure {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        match NonZero::new(reader.long()?.len()) {
            None => Ok(Self),
            Some(actual) => Err(failure::Error::Protocol(Fault::Challenge { actual }).into()),
        }
    }
}

pub struct SecureOk<'a> {
    pub response: long::Long<'a>,
}

impl method::Spec for SecureOk<'_> {
    const ID: method::Id = SECURE_OK;
}

impl method::Args for SecureOk<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_long(self.response);
    }
}

pub struct Tune {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl method::Spec for Tune {
    const ID: method::Id = TUNE;
}

impl Tune {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        Ok(Self {
            channel_max: reader.read::<u16>()?,
            frame_max: reader.read::<u32>()?,
            heartbeat: reader.read::<u16>()?,
        })
    }
}

pub struct TuneOk {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl method::Spec for TuneOk {
    const ID: method::Id = TUNE_OK;
}

impl method::Args for TuneOk {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(self.channel_max);
        out.put_u32(self.frame_max);
        out.put_u16(self.heartbeat);
    }
}

pub struct Open<'a> {
    pub virtual_host: short::Short<'a>,
}

impl method::Spec for Open<'_> {
    const ID: method::Id = OPEN;
}

impl method::Args for Open<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_short(self.virtual_host);
        out.put_short(short::Short::EMPTY);
        out.put_u8(0);
    }
}

pub struct OpenOk;

impl method::Spec for OpenOk {
    const ID: method::Id = OPEN_OK;
}

impl OpenOk {
    pub(crate) fn read(reader: &mut reader::Reader<'_>) -> Result<Self, fields::Decode> {
        let reserved = reader.short()?;
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

pub(crate) struct CloseRef<'a> {
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

pub struct CloseOk;

impl method::Spec for CloseOk {
    const ID: method::Id = CLOSE_OK;
}

impl method::Encode for CloseOk {
    fn write<S: Write + AsMut<[u8]> + ?Sized>(&self, out: &mut S, channel: u16) {
        frame::Encoder::new(out).method(channel, Self::ID.bytes());
    }
}

pub struct Blocked<'d> {
    pub reason: resident::Exact<'d>,
}

impl method::Spec for Blocked<'_> {
    const ID: method::Id = BLOCKED;
}

impl<'d> Blocked<'d> {
    pub(crate) fn retain<'input, R: buffer::Cursor + 'input>(
        payload: &scope::Deferred<'input, 'd, R>,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<Self, failure::Error> {
        let ((), text) = fields::Decoder::<BlockedText>::retain(payload, budget, |decoder| {
            let (text, decoder) = decoder.short(BlockedText::REASON)?;
            Ok((((), text), decoder))
        })?;
        Ok(Self { reason: text })
    }
}

pub struct Unblocked;

impl method::Spec for Unblocked {
    const ID: method::Id = UNBLOCKED;
}
