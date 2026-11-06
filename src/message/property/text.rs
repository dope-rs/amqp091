use std::{mem, num};

use o3::buffer::{self, storage};

use crate::failure::wire::field;
use crate::message::property::{self, Field, TEXT_FIELDS};
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
        property::raw::text(self, Field::ContentType)
    }

    pub fn encoding(&self) -> Option<&str> {
        property::raw::text(self, Field::ContentEncoding)
    }

    pub fn correlation(&self) -> Option<&str> {
        property::raw::text(self, Field::CorrelationId)
    }

    pub fn reply_to(&self) -> Option<&str> {
        property::raw::text(self, Field::ReplyTo)
    }

    pub fn expiration(&self) -> Option<&str> {
        property::raw::text(self, Field::Expiration)
    }

    pub fn message_id(&self) -> Option<&str> {
        property::raw::text(self, Field::Id)
    }

    pub fn kind(&self) -> Option<&str> {
        property::raw::text(self, Field::Kind)
    }

    pub fn user_id(&self) -> Option<&str> {
        property::raw::text(self, Field::UserId)
    }

    pub fn app_id(&self) -> Option<&str> {
        property::raw::text(self, Field::AppId)
    }

    pub fn reserved(&self) -> Option<&str> {
        property::raw::text(self, Field::Reserved)
    }

    pub(super) fn encode(&self, field: Field, output: &mut impl Put) {
        if let Some(value) = property::raw::text(self, field) {
            output.push(value.len() as u8);
            output.extend(value.as_bytes());
        }
    }
}

impl Default for Text<storage::Owned> {
    fn default() -> Self {
        Self {
            bytes: storage::Owned::new(),
            keys: [Key::NONE; TEXT_FIELDS],
        }
    }
}

impl Text<storage::Owned> {
    pub(in crate::message) fn freeze(self) -> Text<storage::Shared> {
        Text::new(self.bytes.freeze(), self.keys)
    }
}

const _: () = assert!(mem::size_of::<Key>() == mem::size_of::<u32>());
