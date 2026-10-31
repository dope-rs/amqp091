use hold::scope;
use o3::buffer;
use o3::buffer::{hold, storage};

use crate::failure::{self, wire};
use crate::message::field::view;
use crate::message::property::{self, source};
use crate::protocol::class;

pub struct Header {
    body_size: u64,
    properties: property::Properties<storage::Shared, view::TableSpan>,
}

pub(crate) struct Draft<'input, 'd> {
    body_size: u64,
    properties: source::State<'input, 'd>,
}

struct Prefix {
    body_size: u64,
    flags: u16,
}

impl Header {
    pub fn decode(payload: storage::Shared) -> Result<Self, failure::Error> {
        let (head, payload) = payload
            .try_split()
            .map_err(|cause| failure::Error::Protocol(wire::Fault::ContentSize(cause)))?;
        let prefix = Prefix::parse(&head)?;
        let props = property::Properties::decode(prefix.flags, payload)?;
        Ok(Self {
            body_size: prefix.body_size,
            properties: props,
        })
    }

    pub const fn body_size(&self) -> u64 {
        self.body_size
    }

    pub const fn properties(&self) -> &property::Properties<storage::Shared, view::TableSpan> {
        &self.properties
    }

    pub fn into_parts(self) -> (u64, property::Properties<storage::Shared, view::TableSpan>) {
        (self.body_size, self.properties)
    }
}

impl Prefix {
    fn parse(header: &[u8; 14]) -> Result<Self, failure::Error> {
        let [
            class_hi,
            class_lo,
            mass_hi,
            mass_lo,
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            flags_hi,
            flags_lo,
        ] = *header;
        let class = u16::from_be_bytes([class_hi, class_lo]);
        if class != class::BASIC.get() {
            return Err(failure::Error::Protocol(wire::Fault::ContentClass {
                actual: class,
            }));
        }
        let weight = u16::from_be_bytes([mass_hi, mass_lo]);
        if weight != 0 {
            return Err(failure::Error::Protocol(wire::Fault::ContentWeight {
                actual: weight,
            }));
        }
        Ok(Self {
            body_size: u64::from_be_bytes([a, b, c, d, e, f, g, h]),
            flags: u16::from_be_bytes([flags_hi, flags_lo]),
        })
    }
}

impl<'input, 'd> Draft<'input, 'd> {
    pub(crate) fn parse<R: buffer::Cursor + 'input>(
        payload: scope::Deferred<'input, 'd, R>,
    ) -> Result<Self, failure::Error> {
        let (head, payload) = payload
            .try_split()
            .map_err(|cause| failure::Error::Protocol(wire::Fault::ContentSize(cause)))?;
        let prefix = Prefix::parse(head)?;
        let props = property::Properties::decode(prefix.flags, source::Source::new(payload))?;
        Ok(Self {
            body_size: prefix.body_size,
            properties: props.into_state(),
        })
    }

    pub(crate) const fn body_size(&self) -> u64 {
        self.body_size
    }

    pub(crate) fn into_state(self) -> source::State<'input, 'd> {
        self.properties
    }
}
