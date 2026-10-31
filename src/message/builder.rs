use std::num;

use o3::buffer::{self, storage};

use crate::failure::wire::property::Fault;
use crate::failure::{self, wire};
use crate::message::{field, property};

#[derive(Default)]
pub struct Builder {
    text: property::Text<Option<storage::Owned<(), { buffer::BLOCK_CAPACITY }>>>,
    headers: Option<field::Table>,
    delivery_mode: Option<num::NonZero<u8>>,
    priority: Option<u8>,
    timestamp: Option<u64>,
}

impl Builder {
    pub fn content_type(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::CONTENT_TYPE, value.as_ref())?;
        Ok(self)
    }

    pub fn encoding(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text
            .set_text(property::CONTENT_ENCODING, value.as_ref())?;
        Ok(self)
    }

    pub fn correlation(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text
            .set_text(property::CORRELATION_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn reply_to(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::REPLY_TO, value.as_ref())?;
        Ok(self)
    }

    pub fn expiration(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::EXPIRATION, value.as_ref())?;
        Ok(self)
    }

    pub fn message_id(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::ID, value.as_ref())?;
        Ok(self)
    }

    pub fn kind(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::KIND, value.as_ref())?;
        Ok(self)
    }

    pub fn user_id(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::USER_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn app_id(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(property::APP_ID, value.as_ref())?;
        Ok(self)
    }

    pub fn reserved(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        if !value.as_ref().is_empty() {
            return Err(failure::Error::Protocol(wire::Fault::Property(
                Fault::Reserved {
                    actual: value.as_ref().len(),
                },
            )));
        }
        self.text.set_text(property::RESERVED, "")?;
        Ok(self)
    }

    pub fn headers(mut self, headers: field::Table) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn delivery(mut self, mode: u8) -> Result<Self, failure::Error> {
        self.delivery_mode = Some(property::try_delivery_mode(mode)?);
        Ok(self)
    }

    pub fn priority(mut self, priority: u8) -> Result<Self, failure::Error> {
        self.priority = Some(property::try_priority(priority)?);
        Ok(self)
    }

    pub fn timestamp(mut self, time: u64) -> Self {
        self.timestamp = Some(time);
        self
    }

    pub fn build(self) -> property::Properties {
        property::Properties {
            text: self.text.freeze(),
            headers: self.headers,
            delivery_mode: self.delivery_mode,
            priority: self.priority,
            timestamp: self.timestamp,
        }
    }
}
