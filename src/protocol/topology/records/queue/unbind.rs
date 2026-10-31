use o3::buffer::resident::Budget;

use crate::failure;
use crate::protocol::topology::records::queue::Key;
use crate::protocol::wire::schema::queues;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, bytes};

pub(crate) struct Unbind<'d, const ID: u8> {
    record: bytes::Bytes<'d, queues::Binding>,
    queue: Option<Key<'d, ID>>,
}

impl<'d, const ID: u8> Unbind<'d, ID> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        queue: Option<Key<'d, ID>>,
        encoded: wire::Encoded<'_, queues::Binding>,
    ) -> Result<Self, failure::Error> {
        Ok(Self {
            record: bytes::Bytes::capture(budget, encoded)?,
            queue,
        })
    }

    pub(in crate::protocol::topology) const fn key(&self) -> Option<Key<'d, ID>> {
        self.queue
    }

    pub(in crate::protocol::topology) fn name(&self) -> short::Short<'_> {
        self.record.field(queues::Binding::QUEUE)
    }

    pub(in crate::protocol::topology) fn exchange(&self) -> short::Short<'_> {
        self.record.field(queues::Binding::EXCHANGE)
    }

    pub(in crate::protocol::topology) fn routing_key(&self) -> short::Short<'_> {
        self.record.field(queues::Binding::ROUTING_KEY)
    }

    pub(in crate::protocol::topology) fn arguments(&self) -> &[u8] {
        self.record.suffix()
    }
}
