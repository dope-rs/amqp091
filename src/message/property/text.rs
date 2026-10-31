use std::{mem, num};

use o3::buffer::write::{Sink as _, SpareExt as _};
use o3::buffer::{self, storage};

use crate::failure::wire::field;
use crate::failure::{self, wire};
use crate::message::property::{
    self, APP_ID, CONTENT_ENCODING, CONTENT_TYPE, CORRELATION_ID, EXPIRATION, ID, KIND, REPLY_TO,
    RESERVED, TEXT_FIELDS, USER_ID,
};
use crate::protocol::wire::Put;

pub struct Text<S> {
    pub(super) bytes: S,
    pub(super) keys: [Key; TEXT_FIELDS],
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Key(pub(super) Option<num::NonZero<u32>>);

impl Key {
    pub(super) const NONE: Self = Self(None);

    pub(super) const fn new(start: u32) -> Self {
        Self(num::NonZero::<u32>::new(start))
    }

    pub(super) fn try_new(start: usize) -> Result<Self, field::Fault> {
        let Ok(start) = u32::try_from(start) else {
            return Err(field::Fault::Text {
                start,
                max: u32::MAX as usize,
            });
        };
        Ok(Self::new(start))
    }

    pub(super) const fn is_some(self) -> bool {
        self.0.is_some()
    }
}

impl<S: buffer::raw::Source> Text<S> {
    pub(super) const fn new(bytes: S, keys: [Key; TEXT_FIELDS]) -> Self {
        Self { bytes, keys }
    }

    pub(super) fn into_parts(self) -> (S, [Key; TEXT_FIELDS]) {
        (self.bytes, self.keys)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.as_ref().is_empty()
    }

    pub fn content_type(&self) -> Option<&str> {
        property::raw::text(self, CONTENT_TYPE)
    }

    pub fn encoding(&self) -> Option<&str> {
        property::raw::text(self, CONTENT_ENCODING)
    }

    pub fn correlation(&self) -> Option<&str> {
        property::raw::text(self, CORRELATION_ID)
    }

    pub fn reply_to(&self) -> Option<&str> {
        property::raw::text(self, REPLY_TO)
    }

    pub fn expiration(&self) -> Option<&str> {
        property::raw::text(self, EXPIRATION)
    }

    pub fn message_id(&self) -> Option<&str> {
        property::raw::text(self, ID)
    }

    pub fn kind(&self) -> Option<&str> {
        property::raw::text(self, KIND)
    }

    pub fn user_id(&self) -> Option<&str> {
        property::raw::text(self, USER_ID)
    }

    pub fn app_id(&self) -> Option<&str> {
        property::raw::text(self, APP_ID)
    }

    pub fn reserved(&self) -> Option<&str> {
        property::raw::text(self, RESERVED)
    }

    pub(super) fn encode(&self, field: usize, output: &mut impl Put) {
        if let Some(value) = property::raw::text(self, field) {
            output.push(value.len() as u8);
            output.extend(value.as_bytes());
        }
    }
}

impl Default for Text<Option<storage::Owned<(), { buffer::BLOCK_CAPACITY }>>> {
    fn default() -> Self {
        Self {
            bytes: None,
            keys: [Key::NONE; TEXT_FIELDS],
        }
    }
}

impl Text<Option<storage::Owned<(), { buffer::BLOCK_CAPACITY }>>> {
    pub(in crate::message) fn set_text(
        &mut self,
        field: usize,
        value: &str,
    ) -> Result<(), failure::Error> {
        if let Some(offset) = value.bytes().position(|byte| byte == 0) {
            return Err(failure::Error::Protocol(wire::Fault::TextNul {
                kind: wire::Encoding::Short,
                offset,
            }));
        }
        if value.len() > u8::MAX as usize {
            return Err(failure::Error::Encoding(wire::Encode::Length {
                kind: wire::Encoding::Short,
                actual: value.len(),
                max: u8::MAX as usize,
            }));
        }
        let len = value.len() as u8;
        let text = match self.bytes.as_mut() {
            Some(text) => text,
            None => self
                .bytes
                .insert(
                    storage::Owned::try_new().map_err(|error| failure::Error::Allocation {
                        target: wire::Allocation::Property,
                        error: error.into(),
                    })?,
                ),
        };
        let start = text.len() as u32 + 1;
        let prefix = [len];
        let mut spare = text.spare();
        if let Err(error) = spare.write_slices([prefix.as_slice(), value.as_bytes()]) {
            return Err(failure::Error::Storage {
                target: wire::Allocation::Property,
                error: storage::AllocError::Capacity(error),
            });
        }
        spare.commit();
        self.keys[field] = Key::new(start);
        Ok(())
    }

    pub(in crate::message) fn freeze(self) -> Text<storage::Shared> {
        Text::new(
            self.bytes.map_or_else(
                storage::Shared::new,
                <storage::Owned<(), { buffer::BLOCK_CAPACITY }>>::freeze,
            ),
            self.keys,
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Key>() == mem::size_of::<u32>());
    assert!(buffer::BLOCK_CAPACITY < u32::MAX);
};
