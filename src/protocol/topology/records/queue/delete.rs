use o3::buffer::resident::Budget;

use crate::failure;
use crate::protocol::topology::records::queue::Key;
use crate::protocol::wire::bytes;
use crate::protocol::wire::text::short;

pub(crate) struct Delete<'d, const ID: u8> {
    record: bytes::Field<'d>,
    queue: Option<Key<'d, ID>>,
}

impl<'d, const ID: u8> Delete<'d, ID> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        queue: Option<Key<'d, ID>>,
        name: short::Name<'_>,
    ) -> Result<Self, failure::Error> {
        Ok(Self {
            record: bytes::Field::capture(budget, name)?,
            queue,
        })
    }

    pub(crate) fn name(&self) -> short::Name<'_> {
        self.record.value()
    }

    pub(crate) const fn queue_key(&self) -> Option<Key<'d, ID>> {
        self.queue
    }
}
