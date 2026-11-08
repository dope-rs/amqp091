use o3::alloc;
use o3::depot::{capacity, map};

use crate::failure::wire::Capacity;
use crate::protocol::topology::records::exchange;
use crate::protocol::topology::store;
use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::text::short;

pub(in crate::protocol::topology) struct Exchanges<'c, 'd, const ID: u8> {
    pub(in crate::protocol::topology) store:
        store::Store<'c, exchange::Declare<'d>, store::Exchange<'d, ID>>,
}

impl<'c, 'd, const ID: u8> Exchanges<'c, 'd, ID> {
    pub(super) fn try_new(
        cap: capacity::Count,
        names: map::Plan,
        permits: &crate::permits::Registry<'c, 'd>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            store: store::Store::try_new(cap, names, permits)?,
        })
    }

    pub(in crate::protocol::topology) fn key(
        &self,
        hash: u64,
        name: short::Short<'_>,
    ) -> Option<store::Exchange<'d, ID>> {
        self.store.key(hash, |item| item.name() == name)
    }

    pub(in crate::protocol::topology) fn record(
        &mut self,
        decl: exchange::Declare<'d>,
        plan: store::Binding<'c, store::Exchange<'d, ID>>,
    ) -> Result<
        (),
        (
            exchange::Declare<'d>,
            store::Binding<'c, store::Exchange<'d, ID>>,
            Capacity,
        ),
    > {
        self.store
            .upsert(plan, decl, |left, right| left.name() == right.name())
    }

    pub(super) fn forget(&mut self, name: short::Short<'_>) {
        let hash = Fingerprint::of(name.value().as_bytes()).get();
        self.store.remove(
            hash,
            |item| item.name() == name,
            |item| Fingerprint::of(item.name().value().as_bytes()).get(),
        );
    }
}
