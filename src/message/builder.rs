use std::num;

use o3::buffer::storage;

use crate::failure::wire::property::Fault;
use crate::failure::{self, wire};
use crate::message::property::Field;
use crate::message::{field, property};

#[derive(Default)]
pub struct Builder {
    text: property::Text<storage::Owned>,
    headers: Option<field::Table>,
    delivery_mode: Option<num::NonZero<u8>>,
    priority: Option<u8>,
    timestamp: Option<u64>,
}

impl Builder {
    pub fn content_type(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::ContentType, value.as_ref())?;
        Ok(self)
    }

    pub fn encoding(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::ContentEncoding, value.as_ref())?;
        Ok(self)
    }

    pub fn correlation(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::CorrelationId, value.as_ref())?;
        Ok(self)
    }

    pub fn reply_to(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::ReplyTo, value.as_ref())?;
        Ok(self)
    }

    pub fn expiration(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::Expiration, value.as_ref())?;
        Ok(self)
    }

    pub fn message_id(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::Id, value.as_ref())?;
        Ok(self)
    }

    pub fn kind(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::Kind, value.as_ref())?;
        Ok(self)
    }

    pub fn user_id(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::UserId, value.as_ref())?;
        Ok(self)
    }

    pub fn app_id(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.text.set_text(Field::AppId, value.as_ref())?;
        Ok(self)
    }

    pub fn reserved(mut self, value: impl AsRef<str>) -> Result<Self, failure::Error> {
        let actual = value.as_ref().len();
        if actual != 0 {
            return Err(failure::Error::Protocol(wire::Fault::Property(
                Fault::Reserved { actual },
            )));
        }
        self.text.set_text(Field::Reserved, "")?;
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
