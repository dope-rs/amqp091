use crate::Error;
use crate::method::ClassId;
use crate::wire::{FieldTable, Put, Reader, Sink};

#[derive(Debug, Clone, Default)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub kind: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub reserved: Option<String>,
}

impl BasicProperties {
    const FLAG_CONTENT_TYPE: u16 = 1 << 15;
    const FLAG_CONTENT_ENCODING: u16 = 1 << 14;
    const FLAG_HEADERS: u16 = 1 << 13;
    const FLAG_DELIVERY_MODE: u16 = 1 << 12;
    const FLAG_PRIORITY: u16 = 1 << 11;
    const FLAG_CORRELATION_ID: u16 = 1 << 10;
    const FLAG_REPLY_TO: u16 = 1 << 9;
    const FLAG_EXPIRATION: u16 = 1 << 8;
    const FLAG_MESSAGE_ID: u16 = 1 << 7;
    const FLAG_TIMESTAMP: u16 = 1 << 6;
    const FLAG_TYPE: u16 = 1 << 5;
    const FLAG_USER_ID: u16 = 1 << 4;
    const FLAG_APP_ID: u16 = 1 << 3;
    const FLAG_RESERVED: u16 = 1 << 2;
    const FLAG_CONTINUATION: u16 = 1;

    pub fn flags(&self) -> u16 {
        let mut f = 0u16;
        if self.content_type.is_some() {
            f |= Self::FLAG_CONTENT_TYPE;
        }
        if self.content_encoding.is_some() {
            f |= Self::FLAG_CONTENT_ENCODING;
        }
        if self.headers.is_some() {
            f |= Self::FLAG_HEADERS;
        }
        if self.delivery_mode.is_some() {
            f |= Self::FLAG_DELIVERY_MODE;
        }
        if self.priority.is_some() {
            f |= Self::FLAG_PRIORITY;
        }
        if self.correlation_id.is_some() {
            f |= Self::FLAG_CORRELATION_ID;
        }
        if self.reply_to.is_some() {
            f |= Self::FLAG_REPLY_TO;
        }
        if self.expiration.is_some() {
            f |= Self::FLAG_EXPIRATION;
        }
        if self.message_id.is_some() {
            f |= Self::FLAG_MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            f |= Self::FLAG_TIMESTAMP;
        }
        if self.kind.is_some() {
            f |= Self::FLAG_TYPE;
        }
        if self.user_id.is_some() {
            f |= Self::FLAG_USER_ID;
        }
        if self.app_id.is_some() {
            f |= Self::FLAG_APP_ID;
        }
        if self.reserved.is_some() {
            f |= Self::FLAG_RESERVED;
        }
        f
    }

    pub fn encode_header_frame<S: Sink>(&self, out: &mut S, body_size: u64) {
        out.put_u16(ClassId::BASIC);
        out.put_u16(0);
        out.put_u64(body_size);
        out.put_u16(self.flags());
        if let Some(v) = &self.content_type {
            out.put_short_str(v);
        }
        if let Some(v) = &self.content_encoding {
            out.put_short_str(v);
        }
        if let Some(t) = &self.headers {
            t.encode(out);
        }
        if let Some(v) = self.delivery_mode {
            out.put_u8(v);
        }
        if let Some(v) = self.priority {
            out.put_u8(v);
        }
        if let Some(v) = &self.correlation_id {
            out.put_short_str(v);
        }
        if let Some(v) = &self.reply_to {
            out.put_short_str(v);
        }
        if let Some(v) = &self.expiration {
            out.put_short_str(v);
        }
        if let Some(v) = &self.message_id {
            out.put_short_str(v);
        }
        if let Some(v) = self.timestamp {
            out.put_u64(v);
        }
        if let Some(v) = &self.kind {
            out.put_short_str(v);
        }
        if let Some(v) = &self.user_id {
            out.put_short_str(v);
        }
        if let Some(v) = &self.app_id {
            out.put_short_str(v);
        }
        if let Some(v) = &self.reserved {
            out.put_short_str(v);
        }
    }
}

#[derive(Debug)]
pub struct ContentHeader {
    pub class_id: u16,
    pub body_size: u64,
    pub properties: BasicProperties,
}

impl ContentHeader {
    pub fn read(payload: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::new(payload);
        let class_id = r.take_u16()?;
        let _weight = r.take_u16()?;
        let body_size = r.take_u64()?;
        let mut flags = r.take_u16()?;
        while flags & BasicProperties::FLAG_CONTINUATION != 0 {
            flags = r.take_u16()?;
        }
        let mut p = BasicProperties::default();
        if flags & BasicProperties::FLAG_CONTENT_TYPE != 0 {
            p.content_type = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_CONTENT_ENCODING != 0 {
            p.content_encoding = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_HEADERS != 0 {
            p.headers = Some(FieldTable::decode(&mut r)?);
        }
        if flags & BasicProperties::FLAG_DELIVERY_MODE != 0 {
            p.delivery_mode = Some(r.take_u8()?);
        }
        if flags & BasicProperties::FLAG_PRIORITY != 0 {
            p.priority = Some(r.take_u8()?);
        }
        if flags & BasicProperties::FLAG_CORRELATION_ID != 0 {
            p.correlation_id = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_REPLY_TO != 0 {
            p.reply_to = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_EXPIRATION != 0 {
            p.expiration = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_MESSAGE_ID != 0 {
            p.message_id = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_TIMESTAMP != 0 {
            p.timestamp = Some(r.take_u64()?);
        }
        if flags & BasicProperties::FLAG_TYPE != 0 {
            p.kind = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_USER_ID != 0 {
            p.user_id = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_APP_ID != 0 {
            p.app_id = Some(r.take_short_str()?.to_string());
        }
        if flags & BasicProperties::FLAG_RESERVED != 0 {
            p.reserved = Some(r.take_short_str()?.to_string());
        }
        Ok(Self {
            class_id,
            body_size,
            properties: p,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Delivery {
    pub method: super::method::basic::Deliver,
    pub properties: BasicProperties,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Returned {
    pub method: super::method::basic::Return,
    pub properties: BasicProperties,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Got {
    pub method: super::method::basic::GetOk,
    pub properties: BasicProperties,
    pub body: Vec<u8>,
}
