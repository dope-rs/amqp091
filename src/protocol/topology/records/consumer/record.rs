use o3::buffer::resident::Budget;
use o3::mem::rc;

use crate::failure;
use crate::protocol::topology::records::queue;
use crate::protocol::wire::schema::consume;
use crate::protocol::wire::text::{resident, short};
use crate::protocol::wire::{self, bytes};

pub(crate) struct Record<'b, 'd, const ID: u8> {
    record: bytes::Bytes<'d, consume::Layout>,
    queue: Option<queue::Key<'d, ID>>,
    consumer_tag: Option<resident::Identity<'b>>,
}

impl<'b, 'd, const ID: u8> Record<'b, 'd, ID> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        queue: Option<queue::Key<'d, ID>>,
        encoded: wire::Encoded<'_, consume::Layout>,
    ) -> Result<Self, failure::Error> {
        let record = bytes::Bytes::capture(budget, encoded)?;
        Ok(Self {
            record,
            queue,
            consumer_tag: None,
        })
    }

    pub(crate) fn name(&self) -> short::Short<'_> {
        self.record.field(consume::Layout::QUEUE)
    }

    pub(crate) fn try_name(&self) -> Result<resident::Short<'b, 'd>, rc::Error> {
        self.record.try_field(consume::Layout::QUEUE)
    }

    pub(crate) const fn key(&self) -> Option<queue::Key<'d, ID>> {
        self.queue
    }

    pub(crate) fn tag(&self) -> short::Short<'_> {
        self.consumer_tag.as_ref().map_or_else(
            || self.record.field(consume::Layout::CONSUMER_TAG),
            short::Short::from,
        )
    }

    pub(crate) fn try_tag(&self) -> Result<resident::Short<'b, 'd>, rc::Error> {
        match self.consumer_tag.as_ref() {
            Some(tag) => Ok(tag.try_clone()?.into()),
            None => self.record.try_field(consume::Layout::CONSUMER_TAG),
        }
    }

    pub(crate) fn set_tag(&mut self, tag: resident::Identity<'b>) {
        self.consumer_tag = Some(tag);
    }

    pub(in crate::protocol) const fn wire(&self) -> &bytes::Bytes<'d, consume::Layout> {
        &self.record
    }
}
