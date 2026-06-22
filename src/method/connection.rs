use super::encode::ClientMethod;
use super::spec::{ClassId, Connection, Method};
use crate::Error;
use crate::wire::{FieldTable, Put, Reader, Sink};

#[derive(Debug)]
pub struct Start {
    pub version_major: u8,
    pub version_minor: u8,
    pub server_properties: FieldTable,
    pub mechanisms: String,
    pub locales: String,
}

impl Method for Start {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::START;
}

impl Start {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let version_major = r.take_u8()?;
        let version_minor = r.take_u8()?;
        let server_properties = FieldTable::decode(r)?;
        let mechanisms = std::str::from_utf8(r.take_long_str()?)
            .map_err(|_| Error::Protocol("invalid utf-8 in mechanisms"))?
            .to_string();
        let locales = std::str::from_utf8(r.take_long_str()?)
            .map_err(|_| Error::Protocol("invalid utf-8 in locales"))?
            .to_string();
        Ok(Self {
            version_major,
            version_minor,
            server_properties,
            mechanisms,
            locales,
        })
    }
}

pub struct StartOk<'a> {
    pub client_properties: &'a FieldTable,
    pub mechanism: &'a str,
    pub response: &'a [u8],
    pub locale: &'a str,
}

impl Method for StartOk<'_> {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::START_OK;
}

impl ClientMethod for StartOk<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        self.client_properties.encode(out);
        out.put_short_str(self.mechanism);
        out.put_long_str(self.response);
        out.put_short_str(self.locale);
    }
}

#[derive(Debug)]
pub struct Secure {
    pub challenge: Vec<u8>,
}

impl Method for Secure {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::SECURE;
}

impl Secure {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            challenge: r.take_long_str()?.to_vec(),
        })
    }
}

pub struct SecureOk<'a> {
    pub response: &'a [u8],
}

impl Method for SecureOk<'_> {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::SECURE_OK;
}

impl ClientMethod for SecureOk<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_long_str(self.response);
    }
}

#[derive(Debug)]
pub struct Tune {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl Method for Tune {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::TUNE;
}

impl Tune {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            channel_max: r.take_u16()?,
            frame_max: r.take_u32()?,
            heartbeat: r.take_u16()?,
        })
    }
}

pub struct TuneOk {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl Method for TuneOk {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::TUNE_OK;
}

impl ClientMethod for TuneOk {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_u16(self.channel_max);
        out.put_u32(self.frame_max);
        out.put_u16(self.heartbeat);
    }
}

pub struct Open<'a> {
    pub virtual_host: &'a str,
}

impl Method for Open<'_> {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::OPEN;
}

impl ClientMethod for Open<'_> {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_short_str(self.virtual_host);
        out.put_short_str("");
        out.put_u8(0);
    }
}

#[derive(Debug)]
pub struct OpenOk;

impl Method for OpenOk {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::OPEN_OK;
}

impl OpenOk {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        let _reserved = r.take_short_str()?;
        Ok(Self)
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
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::CLOSE;
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
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::CLOSE_OK;
}

impl CloseOk {
    pub fn read(_r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

impl ClientMethod for CloseOk {
    fn write_args<S: Sink>(&self, _out: &mut S) {}
}

#[derive(Debug)]
pub struct Blocked {
    pub reason: String,
}

impl Method for Blocked {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::BLOCKED;
}

impl Blocked {
    pub fn read(r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            reason: r.take_short_str()?.to_string(),
        })
    }
}

#[derive(Debug)]
pub struct Unblocked;

impl Method for Unblocked {
    const CLASS_ID: u16 = ClassId::CONNECTION;
    const METHOD_ID: u16 = Connection::UNBLOCKED;
}

impl Unblocked {
    pub fn read(_r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct AuthResponse;

impl AuthResponse {
    pub fn plain(user: &str, password: &str) -> Vec<u8> {
        let mut v = Vec::with_capacity(2 + user.len() + password.len());
        v.push(0);
        v.extend_from_slice(user.as_bytes());
        v.push(0);
        v.extend_from_slice(password.as_bytes());
        v
    }

    pub fn external(identity: &str) -> Vec<u8> {
        identity.as_bytes().to_vec()
    }
}
