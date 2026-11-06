use std::num;

use client::session::ingress;
use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::ready;
use dope::manifold::client::{self, conn};
use hold::scope;
use o3::buffer;
use o3::buffer::storage::provided::account;
use o3::buffer::{hold, resident};
use o3::mem::credit;
use resident::copy;
use wait::queue::Queue;
use wait::{Attempt, Blocked, resource};

use crate::failure::{self, wire};
use crate::message::field::view;
use crate::message::property::{self, source};
use crate::state::channel;

pub(crate) mod delivery;
mod entry;
pub(in crate::service) mod ops;
pub(crate) mod payload;
pub(crate) mod pending;
pub(crate) mod pool;
pub(crate) mod wait;

pub(crate) struct Store<'r, 'c, 'b, 'd, const ID: u8> {
    pub(in crate::service) pool: &'c pool::Pool<'b, 'd>,
    pub(in crate::service) wait: &'b Queue<'d, ID>,
    pub(in crate::service) routes: channel::Stores<'r, 'c, 'b, 'd>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Store<'r, 'c, 'b, 'd, ID> {
    pub(crate) const fn stores(&self) -> channel::Stores<'r, 'c, 'b, 'd> {
        self.routes
    }

    pub(crate) fn budget(&self) -> &'b credit::Ledger {
        self.wait.body()
    }

    pub(crate) fn ingress(&self) -> ingress::Plan<ingress::In<'d>> {
        ingress::Plan::external(
            ingress::Admit::new(self.wait.resident()),
            self.pool.owners(),
        )
    }

    pub(in crate::service) fn retry_staging(
        &self,
        error: &resident::staging::reserve::Error,
    ) -> Option<resource::Resource> {
        use resident::{AllocError, staging::reserve};

        match error {
            reserve::Error::Resident(AllocError::Budget(credit))
            | reserve::Error::Spill {
                error: AllocError::Budget(credit),
                ..
            } if self.wait.can_release() => Some(resource::Resource::Staging(*credit)),
            _ => None,
        }
    }

    pub(crate) const fn pool(&self) -> &'c pool::Pool<'b, 'd> {
        self.pool
    }

    pub(crate) fn compact(
        &self,
        state: source::State<'_, 'd>,
    ) -> Result<property::Properties<resident::Shared<'d>, view::TableSpan>, pending::Error> {
        let source = match state {
            source::State::Empty(props) => return Ok(props),
            source::State::Resident(source) => source,
        };
        let demand = source.demand();
        let capacity = self.wait.resident().capacity();
        if demand.get() > capacity {
            let error = wire::Capacity::ContentProperty {
                attempted: demand.get(),
                capacity,
            };
            return Err(pending::Error::Failure(failure::Error::Capacity(error)));
        }
        source
            .try_compact()
            .map_err(|error| self.admission(wire::Allocation::Property, error))
    }

    pub(crate) fn retain<'input, R: buffer::Cursor + hold::raw::Retain<'d> + 'input>(
        &self,
        source: scope::Bound<'input, 'd, R>,
    ) -> Result<resident::Shared<'d>, pending::Error> {
        let Some(demand) = num::NonZero::<usize>::new(source.ceiling()) else {
            return Ok(resident::Shared::new());
        };
        let capacity = self.wait.resident().capacity();
        if demand.get() > capacity {
            let error = wire::Capacity::InboundContent {
                attempted: demand.get(),
                capacity,
            };
            return Err(pending::Error::Failure(failure::Error::Capacity(error)));
        }
        source
            .try_into()
            .map_err(|error| self.admission(wire::Allocation::Content, error))
    }

    pub(crate) fn discard(&self, body: payload::Payload<'b, 'd>) {
        use wait::release::Release;

        let release = body.release();
        drop(body);
        self.wait.released(Release::payload(release));
    }

    pub(crate) const fn attempt(
        &self,
        conn: conn::Id<'d, KeyTag<ID>>,
        target: ready::Target<'d>,
    ) -> Attempt<'d, ID> {
        Attempt::new(conn, target)
    }

    pub(crate) fn register(&self, blocked: Blocked<'d, ID>) -> Result<(), failure::Error> {
        self.wait
            .register(blocked)
            .map_err(failure::Error::WaitRegister)
    }

    pub(crate) fn wake_route(&self, conn: conn::Id<'d, KeyTag<ID>>) -> Result<(), failure::Error> {
        self.wait
            .wake_route(conn)
            .map_err(failure::Error::WaitRoute)
    }

    pub(crate) fn cancel(&self, conn: conn::Id<'d, KeyTag<ID>>) -> Result<(), failure::Error> {
        self.wait.cancel(conn).map_err(failure::Error::WaitCancel)
    }

    fn admission(&self, target: wire::Allocation, error: hold::Error) -> pending::Error {
        let retry = self.wait.can_release();
        match error {
            hold::Error::Resident(resident::AllocError::Budget(error)) if retry => {
                pending::Error::Retry(resource::Resource::Resident(error))
            }
            hold::Error::Copy(copy::Error::Budget(error)) if retry => {
                pending::Error::Retry(resource::Resource::Copy(error))
            }
            hold::Error::Account(account::Cause::Budget(error)) if retry => {
                pending::Error::Retry(resource::Resource::Account(error))
            }
            error => pending::Error::Failure(failure::Error::Hold { target, error }),
        }
    }
}
