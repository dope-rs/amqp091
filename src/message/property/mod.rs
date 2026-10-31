use std::{mem, num, ops};

use o3::buffer::{self, storage};
use o3::mem::rc;
use wire::field::Fault;

use crate::failure;
use crate::failure::wire::{self, property};
use crate::message::field::{self, view};
use crate::protocol::class;
use crate::protocol::wire::outputs::Plain;
use crate::protocol::wire::{Put as _, Write, reader};

mod raw;
pub(crate) mod source;
mod text;

use text::Key;
pub use text::Text;

pub(super) const CONTENT_TYPE: usize = 0;
pub(super) const CONTENT_ENCODING: usize = 1;
pub(super) const CORRELATION_ID: usize = 2;
pub(super) const REPLY_TO: usize = 3;
pub(super) const EXPIRATION: usize = 4;
pub(super) const ID: usize = 5;
pub(super) const KIND: usize = 6;
pub(super) const USER_ID: usize = 7;
pub(super) const APP_ID: usize = 8;
pub(super) const RESERVED: usize = 9;
pub(super) const TEXT_FIELDS: usize = 10;

pub struct Properties<S = storage::Shared, H = field::Table> {
    pub(super) text: Text<S>,
    pub(super) headers: Option<H>,
    pub(super) delivery_mode: Option<num::NonZero<u8>>,
    pub(super) priority: Option<u8>,
    pub timestamp: Option<u64>,
}

const _: () = {
    assert!(mem::size_of::<Fields<'_>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Properties>() == 15 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Properties<storage::Shared, view::TableSpan>>()
            == 12 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Text<storage::Shared>>()
            == mem::size_of::<(storage::Shared, [Key; TEXT_FIELDS])>()
    );
};

impl<S, H> ops::Deref for Properties<S, H> {
    type Target = Text<S>;

    fn deref(&self) -> &Self::Target {
        &self.text
    }
}

impl<S> Properties<S> {
    pub fn headers(&self) -> Option<view::Table<'_>> {
        self.headers.as_ref().map(view::Table::owned)
    }
}

impl Properties<storage::Shared, view::TableSpan> {
    pub fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            text: Text::new(self.text.bytes.try_clone()?, self.text.keys),
            headers: self.headers,
            delivery_mode: self.delivery_mode,
            priority: self.priority,
            timestamp: self.timestamp,
        })
    }
}

struct Fields<'a> {
    reader: reader::Reader<'a>,
    len: usize,
}

impl<'a> Fields<'a> {
    fn new(reader: reader::Reader<'a>) -> Self {
        Self {
            len: reader.len(),
            reader,
        }
    }

    /// Returns the consumed prefix length; reads retain only original suffixes.
    fn offset(&self) -> usize {
        self.len - self.reader.len()
    }

    fn try_read<const N: usize>(&mut self) -> Result<[u8; N], failure::Error> {
        let (bytes, reader) = self.reader.take_array().map_err(|error| {
            failure::Error::Field(Fault::Read(reader::Fault {
                offset: self.offset(),
                error,
            }))
        })?;
        self.reader = reader;
        Ok(*bytes)
    }

    fn try_short(&mut self) -> Result<(Key, &'a str), failure::Error> {
        let offset = self.offset();
        let value = self
            .reader
            .short()
            .map_err(|error| failure::Error::Field(Fault::Read(reader::Fault { offset, error })))?;
        let start = offset + mem::size_of::<u8>();
        if let Some(at) = value.bytes().position(|byte| byte == 0) {
            return Err(failure::Error::Protocol(wire::Fault::TextNul {
                kind: wire::Encoding::Short,
                offset: start + at,
            }));
        }
        let key = Key::try_new(start).map_err(failure::Error::Field)?;
        Ok((key, value))
    }

    fn try_table(&mut self) -> Result<view::TableSpan, failure::Error> {
        let start = self.offset();
        let body = self.reader.long().map_err(|error| {
            failure::Error::Field(Fault::Read(reader::Fault {
                offset: start,
                error,
            }))
        })?;
        view::TableSpan::try_new(start, body)
    }

    fn finish(self) -> Result<(), failure::Error> {
        let left = self.reader.len();
        if left == 0 {
            Ok(())
        } else {
            Err(failure::Error::Protocol(wire::Fault::Property(
                property::Fault::Trailing {
                    offset: self.offset(),
                    remaining: left,
                },
            )))
        }
    }
}

impl Default for Properties {
    fn default() -> Self {
        Self {
            text: Text::new(storage::Shared::new(), [Key::NONE; TEXT_FIELDS]),
            headers: None,
            delivery_mode: None,
            priority: None,
            timestamp: None,
        }
    }
}

pub(super) fn try_delivery_mode(mode: u8) -> Result<num::NonZero<u8>, failure::Error> {
    match num::NonZero::new(mode) {
        Some(mode) if mode.get() <= 2 => Ok(mode),
        _ => Err(failure::Error::Protocol(wire::Fault::Property(
            property::Fault::DeliveryMode { actual: mode },
        ))),
    }
}

pub(super) fn try_priority(priority: u8) -> Result<u8, failure::Error> {
    match priority {
        0..=9 => Ok(priority),
        actual => Err(failure::Error::Protocol(wire::Fault::Property(
            property::Fault::Priority { actual, max: 9 },
        ))),
    }
}

impl<S, H> Properties<S, H> {
    pub(in crate::message) fn source(&self) -> &S {
        &self.text.bytes
    }

    pub const fn delivery(&self) -> Option<u8> {
        match self.delivery_mode {
            Some(mode) => Some(mode.get()),
            None => None,
        }
    }

    pub const fn priority(&self) -> Option<u8> {
        self.priority
    }
}

impl Properties {
    pub(super) const FLAG_CONTENT_TYPE: u16 = 1 << 15;
    pub(super) const FLAG_CONTENT_ENCODING: u16 = 1 << 14;
    pub(super) const FLAG_HEADERS: u16 = 1 << 13;
    pub(super) const FLAG_DELIVERY_MODE: u16 = 1 << 12;
    pub(super) const FLAG_PRIORITY: u16 = 1 << 11;
    pub(super) const FLAG_CORRELATION_ID: u16 = 1 << 10;
    pub(super) const FLAG_REPLY_TO: u16 = 1 << 9;
    pub(super) const FLAG_EXPIRATION: u16 = 1 << 8;
    pub(super) const FLAG_MESSAGE_ID: u16 = 1 << 7;
    pub(super) const FLAG_TIMESTAMP: u16 = 1 << 6;
    pub(super) const FLAG_TYPE: u16 = 1 << 5;
    pub(super) const FLAG_USER_ID: u16 = 1 << 4;
    pub(super) const FLAG_APP_ID: u16 = 1 << 3;
    pub(super) const FLAG_RESERVED: u16 = 1 << 2;
    pub(super) const KNOWN_FLAGS: u16 = Self::FLAG_CONTENT_TYPE
        | Self::FLAG_CONTENT_ENCODING
        | Self::FLAG_HEADERS
        | Self::FLAG_DELIVERY_MODE
        | Self::FLAG_PRIORITY
        | Self::FLAG_CORRELATION_ID
        | Self::FLAG_REPLY_TO
        | Self::FLAG_EXPIRATION
        | Self::FLAG_MESSAGE_ID
        | Self::FLAG_TIMESTAMP
        | Self::FLAG_TYPE
        | Self::FLAG_USER_ID
        | Self::FLAG_APP_ID
        | Self::FLAG_RESERVED;

    pub(crate) fn decode<S: buffer::raw::Source>(
        flags: u16,
        text: S,
    ) -> Result<Properties<S, view::TableSpan>, failure::Error> {
        if flags & !Self::KNOWN_FLAGS != 0 {
            return Err(failure::Error::Protocol(wire::Fault::Property(
                property::Fault::Flags {
                    actual: flags,
                    allowed: Self::KNOWN_FLAGS,
                },
            )));
        }
        let mut decoded = Properties {
            text: Text::new(text, [Key::NONE; TEXT_FIELDS]),
            headers: None,
            delivery_mode: None,
            priority: None,
            timestamp: None,
        };
        let mut input = Fields::new(reader::Reader::new(decoded.text.bytes.as_ref()));
        if flags & Self::FLAG_CONTENT_TYPE != 0 {
            decoded.text.keys[CONTENT_TYPE] = input.try_short()?.0;
        }
        if flags & Self::FLAG_CONTENT_ENCODING != 0 {
            decoded.text.keys[CONTENT_ENCODING] = input.try_short()?.0;
        }
        if flags & Self::FLAG_HEADERS != 0 {
            decoded.headers = Some(input.try_table()?);
        }
        if flags & Self::FLAG_DELIVERY_MODE != 0 {
            decoded.delivery_mode = Some(try_delivery_mode(u8::from_be_bytes(input.try_read()?))?);
        }
        if flags & Self::FLAG_PRIORITY != 0 {
            decoded.priority = Some(try_priority(u8::from_be_bytes(input.try_read()?))?);
        }
        if flags & Self::FLAG_CORRELATION_ID != 0 {
            decoded.text.keys[CORRELATION_ID] = input.try_short()?.0;
        }
        if flags & Self::FLAG_REPLY_TO != 0 {
            decoded.text.keys[REPLY_TO] = input.try_short()?.0;
        }
        if flags & Self::FLAG_EXPIRATION != 0 {
            decoded.text.keys[EXPIRATION] = input.try_short()?.0;
        }
        if flags & Self::FLAG_MESSAGE_ID != 0 {
            decoded.text.keys[ID] = input.try_short()?.0;
        }
        if flags & Self::FLAG_TIMESTAMP != 0 {
            decoded.timestamp = Some(u64::from_be_bytes(input.try_read()?));
        }
        if flags & Self::FLAG_TYPE != 0 {
            decoded.text.keys[KIND] = input.try_short()?.0;
        }
        if flags & Self::FLAG_USER_ID != 0 {
            decoded.text.keys[USER_ID] = input.try_short()?.0;
        }
        if flags & Self::FLAG_APP_ID != 0 {
            decoded.text.keys[APP_ID] = input.try_short()?.0;
        }
        if flags & Self::FLAG_RESERVED != 0 {
            let (key, value) = input.try_short()?;
            if !value.is_empty() {
                return Err(failure::Error::Protocol(wire::Fault::Property(
                    property::Fault::Reserved {
                        actual: value.len(),
                    },
                )));
            }
            decoded.text.keys[RESERVED] = key;
        }
        input.finish()?;
        Ok(decoded)
    }
}

pub(crate) struct Encoder<'a>(&'a Properties);

impl<'a> Encoder<'a> {
    pub(crate) const fn new(props: &'a Properties) -> Self {
        Self(props)
    }
}

impl ops::Deref for Encoder<'_> {
    type Target = Properties;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl Encoder<'_> {
    fn flags(&self) -> u16 {
        let mut f = 0u16;
        if self.text.keys[CONTENT_TYPE].is_some() {
            f |= Properties::FLAG_CONTENT_TYPE;
        }
        if self.text.keys[CONTENT_ENCODING].is_some() {
            f |= Properties::FLAG_CONTENT_ENCODING;
        }
        if self.headers.is_some() {
            f |= Properties::FLAG_HEADERS;
        }
        if self.delivery_mode.is_some() {
            f |= Properties::FLAG_DELIVERY_MODE;
        }
        if self.priority.is_some() {
            f |= Properties::FLAG_PRIORITY;
        }
        if self.text.keys[CORRELATION_ID].is_some() {
            f |= Properties::FLAG_CORRELATION_ID;
        }
        if self.text.keys[REPLY_TO].is_some() {
            f |= Properties::FLAG_REPLY_TO;
        }
        if self.text.keys[EXPIRATION].is_some() {
            f |= Properties::FLAG_EXPIRATION;
        }
        if self.text.keys[ID].is_some() {
            f |= Properties::FLAG_MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            f |= Properties::FLAG_TIMESTAMP;
        }
        if self.text.keys[KIND].is_some() {
            f |= Properties::FLAG_TYPE;
        }
        if self.text.keys[USER_ID].is_some() {
            f |= Properties::FLAG_USER_ID;
        }
        if self.text.keys[APP_ID].is_some() {
            f |= Properties::FLAG_APP_ID;
        }
        if self.text.keys[RESERVED].is_some() {
            f |= Properties::FLAG_RESERVED;
        }
        f
    }

    pub(crate) fn encoded_len(&self) -> u64 {
        let text = (0..TEXT_FIELDS)
            .filter_map(|field| raw::text(&self.text, field))
            .map(|value| 1 + value.len() as u64)
            .sum::<u64>();
        let headers = self
            .headers()
            .map_or(0, |table| 4 + u64::from(table.encoded_len()));
        let octets = u64::from(self.delivery_mode.is_some()) + u64::from(self.priority.is_some());
        2 + 2 + 8 + 2 + text + headers + octets + u64::from(self.timestamp.is_some()) * 8
    }

    pub(crate) fn encode<W: Write>(&self, out: &mut W, size: u64) {
        let mut out = Plain::new(out);
        let out = &mut out;
        out.put_u16(class::BASIC.get());
        out.put_u16(0);
        out.put_u64(size);
        out.put_u16(self.flags());
        self.text.encode(CONTENT_TYPE, out);
        self.text.encode(CONTENT_ENCODING, out);
        if let Some(t) = self.headers() {
            t.encode(out);
        }
        if let Some(v) = self.delivery_mode {
            out.put_u8(v.get());
        }
        if let Some(v) = self.priority {
            out.put_u8(v);
        }
        self.text.encode(CORRELATION_ID, out);
        self.text.encode(REPLY_TO, out);
        self.text.encode(EXPIRATION, out);
        self.text.encode(ID, out);
        if let Some(v) = self.timestamp {
            out.put_u64(v);
        }
        self.text.encode(KIND, out);
        self.text.encode(USER_ID, out);
        self.text.encode(APP_ID, out);
        self.text.encode(RESERVED, out);
    }
}
