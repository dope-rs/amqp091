use o3::buffer::resident::Budget;

use crate::failure;
use crate::protocol::topology::records::queue::Key;
use crate::protocol::wire::schema::queues;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, bytes};

pub(crate) struct Bind<'d, const ID: u8> {
    record: bytes::Bytes<'d, queues::Binding>,
    queue: Option<Key<'d, ID>>,
}

impl<'d, const ID: u8> Bind<'d, ID> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        queue: Option<Key<'d, ID>>,
        encoded: wire::Encoded<'_, queues::Binding>,
    ) -> Result<Self, failure::Error> {
        let record = bytes::Bytes::capture(budget, encoded)?;
        Ok(Self { record, queue })
    }

    pub(crate) fn name(&self) -> short::Short<'_> {
        self.record.field(queues::Binding::QUEUE)
    }

    pub(crate) const fn key(&self) -> Option<Key<'d, ID>> {
        self.queue
    }

    pub(in crate::protocol::topology) fn matches(
        &self,
        queue: Option<Key<'d, ID>>,
        name: short::Short<'_>,
        source: short::Short<'_>,
        route: short::Short<'_>,
        args: &[u8],
    ) -> bool {
        let target = match (self.queue, queue) {
            (Some(left), Some(right)) => left == right,
            (None, None) => self.name() == name,
            _ => false,
        };
        target
            && self.exchange() == source
            && self.routing_key() == route
            && self.arguments() == args
    }

    pub(crate) fn exchange(&self) -> short::Short<'_> {
        self.record.field(queues::Binding::EXCHANGE)
    }

    pub(crate) fn routing_key(&self) -> short::Short<'_> {
        self.record.field(queues::Binding::ROUTING_KEY)
    }

    pub(in crate::protocol::topology) fn arguments(&self) -> &[u8] {
        self.record.suffix()
    }

    pub(in crate::protocol) const fn wire(&self) -> &bytes::Bytes<'d, queues::Binding> {
        &self.record
    }
}
