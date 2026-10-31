use o3::buffer::resident::Budget;
use o3::mem::rc;

use crate::failure;
use crate::protocol::wire::schema::queues;
use crate::protocol::wire::text::{resident, short};
use crate::protocol::wire::{self, bytes};

pub(crate) struct Declare<'b, 'd> {
    record: bytes::Bytes<'d, queues::Declaration>,
    pub(in crate::protocol::topology) server_name: Option<resident::Identity<'b>>,
}

impl<'b, 'd> Declare<'b, 'd> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        encoded: wire::Encoded<'_, queues::Declaration>,
    ) -> Result<Self, failure::Error> {
        let record = bytes::Bytes::capture(budget, encoded)?;
        Ok(Self {
            record,
            server_name: None,
        })
    }

    pub(crate) fn user_name(&self) -> short::Short<'_> {
        self.record.field(queues::Declaration::QUEUE)
    }

    pub(crate) fn server_name(&self) -> short::Short<'_> {
        self.server_name
            .as_ref()
            .map_or_else(|| self.user_name(), short::Short::from)
    }

    pub(crate) fn try_user(&self) -> Result<resident::Short<'b, 'd>, rc::Error> {
        self.record.try_field(queues::Declaration::QUEUE)
    }

    pub(crate) fn try_name(&self) -> Result<resident::Short<'b, 'd>, rc::Error> {
        match self.server_name.as_ref() {
            Some(name) => Ok(name.try_clone()?.into()),
            None => self.try_user(),
        }
    }

    pub(in crate::protocol) const fn wire(&self) -> &bytes::Bytes<'d, queues::Declaration> {
        &self.record
    }
}
