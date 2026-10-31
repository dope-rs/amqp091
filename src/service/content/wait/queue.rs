use std::marker;

use dope::core::driver;
use dope::core::driver::route::KeyTag;
use dope::manifold::client::conn;
use dope::net::link::egress::metadata;
use o3::alloc;
use o3::base::marker::ThreadBound;
use o3::buffer::resident;
use o3::depot::capacity;
use o3::depot::queue::round;
use o3::mem::credit;

use crate::service::content::pending;
use crate::service::content::wait::Blocked;
use crate::service::content::wait::core::Store;
use crate::service::content::wait::release::Release;
use crate::service::content::wait::wake::Wake;

pub(in crate::service) struct Queue<'d, const ID: u8> {
    store: Store<'d>,
    route: marker::PhantomData<(conn::Lane<'d, KeyTag<ID>>, ThreadBound)>,
}

impl<'d, const ID: u8> Queue<'d, ID> {
    pub(in crate::service) fn try_new(
        capacity: metadata::LaneCount,
        bytes: usize,
        fragments: capacity::Count,
        resident: resident::Budget<'d>,
        owner: driver::Reference<'d>,
    ) -> Result<Self, alloc::Error> {
        let store = Store::try_new(capacity, bytes, fragments, resident, owner)?;
        Ok(Self {
            store,
            route: marker::PhantomData,
        })
    }

    pub(in crate::service::content) fn body(&self) -> &credit::Ledger {
        self.store.body()
    }

    pub(in crate::service::content) fn body_exhausted(
        &self,
        error: credit::Error,
    ) -> pending::Error {
        self.store.body_exhausted(error)
    }

    pub(in crate::service::content) fn resident(&self) -> resident::Budget<'d> {
        self.store.resident()
    }

    pub(in crate::service::content) fn can_release(&self) -> bool {
        self.store.can_release()
    }

    pub(in crate::service::content) fn fragment(&self) -> pending::Error {
        self.store.fragment()
    }

    pub(in crate::service::content) fn register(
        &self,
        blocked: Blocked<'d, ID>,
    ) -> Result<(), round::Bounds> {
        let (id, target, resource) = blocked.into_parts();
        self.store
            .register(id.lane(), conn::Lifetime::generation(id), resource, target)
    }

    pub(in crate::service::content) fn cancel(
        &self,
        id: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<(), round::Bounds> {
        self.store.cancel(id.lane(), conn::Lifetime::generation(id))
    }

    pub(in crate::service) fn wake_route(
        &self,
        id: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<(), round::Bounds> {
        self.store
            .wake_route(id.lane(), conn::Lifetime::generation(id))
    }

    pub(in crate::service::content) fn released(&self, release: Release) {
        self.store.released(release);
    }

    pub(in crate::service) fn wake(&self) -> Wake<'_, 'd> {
        Wake::new(&self.store)
    }
}
