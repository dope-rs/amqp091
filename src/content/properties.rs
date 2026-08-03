use std::error;
use std::fmt;
use std::str::from_utf8_unchecked;

use o3::buffer::{CapacityError, Shared};

use super::Body;
use crate::error::Error;
use crate::method::basic::{Deliver, GetOk, Return};
use crate::method::core::ClassId;
use crate::wire::FrameBuffer;
use crate::wire::field::FieldTable;
use crate::wire::put::Put;
use crate::wire::reader::Reader;
use crate::wire::sink::Sink;

const CONTENT_TYPE: usize = 0;
const CONTENT_ENCODING: usize = 1;
const CORRELATION_ID: usize = 2;
const REPLY_TO: usize = 3;
const EXPIRATION: usize = 4;
const MESSAGE_ID: usize = 5;
const KIND: usize = 6;
const USER_ID: usize = 7;
const APP_ID: usize = 8;
const RESERVED: usize = 9;
const TEXT_FIELDS: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TextKey(u32);

impl TextKey {
    const NONE: Self = Self(u32::MAX);

    fn from_start(start: usize) -> Self {
        debug_assert!(start > 0 && u32::try_from(start).is_ok());
        Self(start as u32)
    }

    fn get(self, text: &[u8]) -> Option<&str> {
        if self == Self::NONE {
            return None;
        }
        let start = self.0 as usize;
        let len = unsafe { *text.get_unchecked(start - 1) } as usize;
        let bytes = unsafe { text.get_unchecked(start..start + len) };
        Some(unsafe { from_utf8_unchecked(bytes) })
    }
}

#[derive(Debug, Clone)]
pub struct BasicProperties {
    text: Shared,
    text_keys: [TextKey; TEXT_FIELDS],
    headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub timestamp: Option<u64>,
}

impl Default for BasicProperties {
    fn default() -> Self {
        Self {
            text: Shared::new(),
            text_keys: [TextKey::NONE; TEXT_FIELDS],
            headers: None,
            delivery_mode: None,
            priority: None,
            timestamp: None,
        }
    }
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

    pub fn builder() -> BasicPropertiesBuilder {
        BasicPropertiesBuilder::default()
    }

    fn text(&self, field: usize) -> Option<&str> {
        self.text_keys[field].get(&self.text)
    }

    pub fn content_type(&self) -> Option<&str> {
        self.text(CONTENT_TYPE)
    }

    pub fn content_encoding(&self) -> Option<&str> {
        self.text(CONTENT_ENCODING)
    }

    pub fn headers(&self) -> Option<&FieldTable> {
        self.headers.as_ref()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.text(CORRELATION_ID)
    }

    pub fn reply_to(&self) -> Option<&str> {
        self.text(REPLY_TO)
    }

    pub fn expiration(&self) -> Option<&str> {
        self.text(EXPIRATION)
    }

    pub fn message_id(&self) -> Option<&str> {
        self.text(MESSAGE_ID)
    }

    pub fn kind(&self) -> Option<&str> {
        self.text(KIND)
    }

    pub fn user_id(&self) -> Option<&str> {
        self.text(USER_ID)
    }

    pub fn app_id(&self) -> Option<&str> {
        self.text(APP_ID)
    }

    pub fn reserved(&self) -> Option<&str> {
        self.text(RESERVED)
    }

    pub fn flags(&self) -> u16 {
        let mut f = 0u16;
        if self.content_type().is_some() {
            f |= Self::FLAG_CONTENT_TYPE;
        }
        if self.content_encoding().is_some() {
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
        if self.correlation_id().is_some() {
            f |= Self::FLAG_CORRELATION_ID;
        }
        if self.reply_to().is_some() {
            f |= Self::FLAG_REPLY_TO;
        }
        if self.expiration().is_some() {
            f |= Self::FLAG_EXPIRATION;
        }
        if self.message_id().is_some() {
            f |= Self::FLAG_MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            f |= Self::FLAG_TIMESTAMP;
        }
        if self.kind().is_some() {
            f |= Self::FLAG_TYPE;
        }
        if self.user_id().is_some() {
            f |= Self::FLAG_USER_ID;
        }
        if self.app_id().is_some() {
            f |= Self::FLAG_APP_ID;
        }
        if self.reserved().is_some() {
            f |= Self::FLAG_RESERVED;
        }
        f
    }

    pub fn encode_header_frame<S: Sink>(&self, out: &mut S, body_size: u64) {
        out.put_u16(ClassId::BASIC);
        out.put_u16(0);
        out.put_u64(body_size);
        out.put_u16(self.flags());
        if let Some(v) = self.content_type() {
            out.put_short_str(v);
        }
        if let Some(v) = self.content_encoding() {
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
        if let Some(v) = self.correlation_id() {
            out.put_short_str(v);
        }
        if let Some(v) = self.reply_to() {
            out.put_short_str(v);
        }
        if let Some(v) = self.expiration() {
            out.put_short_str(v);
        }
        if let Some(v) = self.message_id() {
            out.put_short_str(v);
        }
        if let Some(v) = self.timestamp {
            out.put_u64(v);
        }
        if let Some(v) = self.kind() {
            out.put_short_str(v);
        }
        if let Some(v) = self.user_id() {
            out.put_short_str(v);
        }
        if let Some(v) = self.app_id() {
            out.put_short_str(v);
        }
        if let Some(v) = self.reserved() {
            out.put_short_str(v);
        }
    }
}

pub struct BasicPropertiesBuilder {
    text: FrameBuffer,
    text_keys: [TextKey; TEXT_FIELDS],
    headers: Option<FieldTable>,
    delivery_mode: Option<u8>,
    priority: Option<u8>,
    timestamp: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PropertyBuildError {
    ShortString(usize),
    Capacity(CapacityError),
}

impl From<CapacityError> for PropertyBuildError {
    fn from(error: CapacityError) -> Self {
        Self::Capacity(error)
    }
}

impl fmt::Display for PropertyBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShortString(len) => write!(formatter, "AMQP short string is {len} bytes"),
            Self::Capacity(error) => error.fmt(formatter),
        }
    }
}

impl error::Error for PropertyBuildError {}

impl Default for BasicPropertiesBuilder {
    fn default() -> Self {
        Self {
            text: FrameBuffer::new(),
            text_keys: [TextKey::NONE; TEXT_FIELDS],
            headers: None,
            delivery_mode: None,
            priority: None,
            timestamp: None,
        }
    }
}

impl BasicPropertiesBuilder {
    fn set_text(&mut self, field: usize, value: &str) -> Result<(), PropertyBuildError> {
        let len =
            u8::try_from(value.len()).map_err(|_| PropertyBuildError::ShortString(value.len()))?;
        let start = self.text.len() + 1;
        let prefix = [len];
        self.text
            .try_extend_from_slices([prefix.as_slice(), value.as_bytes()])?;
        self.text_keys[field] = TextKey::from_start(start);
        Ok(())
    }

    pub fn content_type(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(CONTENT_TYPE, value.as_ref())?;
        Ok(self)
    }

    pub fn content_encoding(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(CONTENT_ENCODING, value.as_ref())?;
        Ok(self)
    }

    pub fn correlation_id(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(CORRELATION_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn reply_to(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(REPLY_TO, value.as_ref())?;
        Ok(self)
    }

    pub fn expiration(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(EXPIRATION, value.as_ref())?;
        Ok(self)
    }

    pub fn message_id(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(MESSAGE_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn kind(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(KIND, value.as_ref())?;
        Ok(self)
    }

    pub fn user_id(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(USER_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn app_id(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(APP_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn reserved(mut self, value: impl AsRef<str>) -> Result<Self, PropertyBuildError> {
        self.set_text(RESERVED, value.as_ref())?;
        Ok(self)
    }

    pub fn headers(mut self, headers: FieldTable) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn delivery_mode(mut self, delivery_mode: u8) -> Self {
        self.delivery_mode = Some(delivery_mode);
        self
    }

    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn build(self) -> BasicProperties {
        BasicProperties {
            text: self.text.freeze(),
            text_keys: self.text_keys,
            headers: self.headers,
            delivery_mode: self.delivery_mode,
            priority: self.priority,
            timestamp: self.timestamp,
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
    pub fn read(payload: Shared) -> Result<Self, Error> {
        let (class_id, body_size, mut p) = {
            let mut r = Reader::new(&payload);
            let class_id = r.take_u16()?;
            let _weight = r.take_u16()?;
            let body_size = r.take_u64()?;
            let mut flags = r.take_u16()?;
            while flags & BasicProperties::FLAG_CONTINUATION != 0 {
                flags = r.take_u16()?;
            }
            let mut p = BasicProperties::default();
            if flags & BasicProperties::FLAG_CONTENT_TYPE != 0 {
                p.text_keys[CONTENT_TYPE] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_CONTENT_ENCODING != 0 {
                p.text_keys[CONTENT_ENCODING] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
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
                p.text_keys[CORRELATION_ID] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_REPLY_TO != 0 {
                p.text_keys[REPLY_TO] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_EXPIRATION != 0 {
                p.text_keys[EXPIRATION] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_MESSAGE_ID != 0 {
                p.text_keys[MESSAGE_ID] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_TIMESTAMP != 0 {
                p.timestamp = Some(r.take_u64()?);
            }
            if flags & BasicProperties::FLAG_TYPE != 0 {
                p.text_keys[KIND] = TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_USER_ID != 0 {
                p.text_keys[USER_ID] = TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_APP_ID != 0 {
                p.text_keys[APP_ID] = TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            if flags & BasicProperties::FLAG_RESERVED != 0 {
                p.text_keys[RESERVED] =
                    TextKey::from_start(r.take_short_str_range(&payload)?.start);
            }
            (class_id, body_size, p)
        };
        p.text = payload;
        Ok(Self {
            class_id,
            body_size,
            properties: p,
        })
    }
}

#[derive(Debug)]
pub struct Delivery<'d> {
    pub method: Deliver,
    pub properties: BasicProperties,
    pub body: Body<'d>,
}

impl Delivery<'static> {
    pub(crate) unsafe fn assume_lifetime<'d>(self) -> Delivery<'d> {
        Delivery {
            method: self.method,
            properties: self.properties,
            body: unsafe { self.body.assume_lifetime() },
        }
    }
}

#[derive(Debug)]
pub struct Returned<'d> {
    pub method: Return,
    pub properties: BasicProperties,
    pub body: Body<'d>,
}

impl Returned<'static> {
    pub(crate) unsafe fn assume_lifetime<'d>(self) -> Returned<'d> {
        Returned {
            method: self.method,
            properties: self.properties,
            body: unsafe { self.body.assume_lifetime() },
        }
    }
}

#[derive(Debug)]
pub struct Got<'d> {
    pub method: GetOk,
    pub properties: BasicProperties,
    pub body: Body<'d>,
}
