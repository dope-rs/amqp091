use o3::buffer::resident::Budget;

use crate::failure;
use crate::protocol::wire::schema::exchange;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, bytes};

pub(crate) struct Declare<'d> {
    record: bytes::Bytes<'d, exchange::Declare>,
}

impl<'d> Declare<'d> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        encoded: wire::Encoded<'_, exchange::Declare>,
    ) -> Result<Self, failure::Error> {
        let record = bytes::Bytes::capture(budget, encoded)?;
        Ok(Self { record })
    }

    pub(crate) fn name(&self) -> short::Short<'_> {
        self.record.field(exchange::Declare::NAME)
    }

    pub(in crate::protocol) const fn wire(&self) -> &bytes::Bytes<'d, exchange::Declare> {
        &self.record
    }
}
