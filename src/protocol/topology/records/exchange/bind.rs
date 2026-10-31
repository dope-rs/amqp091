use o3::buffer::resident::Budget;

use crate::failure;
use crate::protocol::wire::schema::exchange;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, bytes};

pub(crate) struct Bind<'d> {
    record: bytes::Bytes<'d, exchange::Bind>,
}

impl<'d> Bind<'d> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        encoded: wire::Encoded<'_, exchange::Bind>,
    ) -> Result<Self, failure::Error> {
        let record = bytes::Bytes::capture(budget, encoded)?;
        Ok(Self { record })
    }

    pub(crate) fn destination(&self) -> short::Short<'_> {
        self.record.field(exchange::Bind::DESTINATION)
    }

    pub(crate) fn source(&self) -> short::Short<'_> {
        self.record.field(exchange::Bind::SOURCE)
    }

    pub(crate) fn routing_key(&self) -> short::Short<'_> {
        self.record.field(exchange::Bind::ROUTING_KEY)
    }

    pub(in crate::protocol::topology) fn arguments(&self) -> &[u8] {
        self.record.suffix()
    }

    pub(in crate::protocol::topology) fn matches(
        &self,
        dest: short::Short<'_>,
        source: short::Short<'_>,
        route: short::Short<'_>,
        args: &[u8],
    ) -> bool {
        self.destination() == dest
            && self.source() == source
            && self.routing_key() == route
            && self.arguments() == args
    }

    pub(in crate::protocol) const fn wire(&self) -> &bytes::Bytes<'d, exchange::Bind> {
        &self.record
    }
}
