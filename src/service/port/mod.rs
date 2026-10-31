use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::clock::timer;
use dope::core::driver::{self};
use dope::manifold::client::port;
use dope::manifold::client::session::{self, ingress};
use dope::manifold::service::bindings::{self, failures};
use dope::net::link::egress::metadata;
use dope::net::link::egress::metadata::arena;
use o3::buffer::pool;
use o3::depot;
use o3::depot::arena::slab;

use crate::failure::terminal;
use crate::protocol::wire::text::resident;
use crate::service::{self, cancel, content};
use crate::state::channel::consumer::delivery;
use crate::state::channel::{self, returns};
use crate::state::shared;
use crate::{config, failure, permits};

mod coord;
mod domain;

pub(crate) use coord::Coord;
pub(crate) use domain::{Admission, Domain, EpochAccess, Replay, Waiters};

pub(crate) enum Admit<R> {
    Item(R),
    Unavailable,
    Inactive,
    Error(failure::Error),
}

/// Borrowed storage root for one AMQP route.
/// Runtime clients receive a one-pointer capability, never this root.
pub struct Port<'r, 'c, 'b, 'd, const ID: u8 = 0> {
    pub(in crate::service) domain: Domain<'r, 'c, 'b, 'd, ID>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Port<'r, 'c, 'b, 'd, ID> {
    /// Binds protocol state to this exact AMQP storage owner.
    pub fn bind<'o, F>(
        &'o self,
        failure: F,
    ) -> session::Setup<
        'd,
        bindings::Bound<
            'o,
            'd,
            Self,
            service::Logic<'o, 'd, ID>,
            KeyTag<ID>,
            1,
            failures::External<F>,
        >,
        ingress::In<'d>,
    > {
        let ingress = self.domain.content.ingress();
        let session = bindings::Bound::bind_with(self, service::Logic::new(self), failure);
        session::Setup::new(session, ingress)
    }

    /// Runs the consumer while each borrowed AMQP storage owner remains alive.
    pub fn try_with<C, R>(
        spec: service::Spec<ID>,
        state: &mut C,
        mut driver: impl for<'a> FnMut(&'a mut C) -> driver::Context<'a, 'd>,
        run: impl for<'e, 'f, 'g> FnOnce(Port<'e, 'f, 'g, 'd, ID>, &mut C) -> R,
    ) -> Result<R, service::Build> {
        use content::pool::Pool;
        use content::wait::queue::Queue;
        use domain::egress::Egress;

        let service::Spec { config, plan, .. } = spec;
        let staging = plan.transport.staging;
        let mut backing = arena::Store::try_lanes(staging.retained(), staging.capacity())
            .map_err(service::Build::Allocation)?;
        let compact =
            pool::Pool::try_from(plan.content.compact).map_err(service::Build::Compact)?;
        let parts = depot::raw::Arena::try_new(plan.content.owners.fragments)
            .map_err(service::Build::Allocation)?;
        let ids = resident::Pool::try_new(plan.identities).map_err(service::Build::Allocation)?;
        let (owner, budget, topology) = {
            let context = driver(state);
            let budget = context
                .storage
                .allocator()
                .try_budget(plan.content.bytes)
                .map_err(service::Build::Resident)?;
            let topology = context
                .storage
                .allocator()
                .try_budget(config.capacity.topology.bytes)
                .map_err(service::Build::Resident)?;
            (context.driver_ref(), budget, topology)
        };
        let wait = Queue::try_new(
            staging.capacity(),
            plan.content.bytes,
            plan.content.owners.fragments,
            budget,
            owner,
        )
        .map_err(service::Build::WaitEntries)?;
        let content = Pool::try_new(plan.content.owners, &compact, &parts, wait.wake())
            .map_err(service::Build::Allocation)?;
        let causes =
            terminal::Pool::try_new(plan.transport.causes).map_err(service::Build::Allocation)?;
        let queues =
            slab::Cell::<_, _, { u32::MAX }, false>::try_new(plan.channels.topology().queues())
                .map_err(service::Build::Allocation)?;
        let waiters = Waiters::try_new(owner, plan.coordination.waiter_capacity)?;
        let cancel = cancel::Registry::try_new(plan.coordination.cancellations)
            .map_err(service::Build::Allocation)?;
        let regular = pool::Pool::try_from(plan.egress.regular).map_err(service::Build::Regular)?;
        let control = pool::Pool::try_from(plan.egress.control).map_err(service::Build::Control)?;
        permits::Registry::try_with(
            owner,
            plan.coordination.admissions,
            (&content, &wait, &ids, &causes, &queues),
            |(content, wait, ids, causes, queues), admit| {
                let routes = delivery::Store::try_new(plan.content.delivery_routes)
                    .map_err(service::Build::Allocation)?;
                let returns = returns::Store::try_new(
                    plan.content.return_routes,
                    plan.channels.slots(),
                    owner,
                )?;
                let context = driver(state);
                let link = port::Port::try_new(&mut backing, context.storage.region())
                    .map_err(service::Build::Allocation)?;
                let session = shared::Root::try_new(
                    plan.channels,
                    queues,
                    owner,
                    context.storage.region(),
                    &admit,
                )?;
                let port = Port {
                    domain: Domain {
                        transport: link,
                        session,
                        coordination: service::Coord {
                            waiters,
                            cancellations: cancel,
                        },
                        admissions: admit,
                        egress: Egress::new(regular, control),
                        content: content::Store {
                            pool: content,
                            wait,
                            routes: channel::Stores::new(&routes, &returns),
                        },
                        storage: domain::Storage {
                            budget: topology,
                            identities: ids,
                            causes,
                        },
                        settings: domain::Settings {
                            timer: owner.timer(),
                            config,
                        },
                    },
                };
                Ok(run(port, state))
            },
        )?
    }

    pub(crate) const fn domain(&self) -> &Domain<'r, 'c, 'b, 'd, ID> {
        &self.domain
    }

    pub fn capacity(&self) -> metadata::LaneCount {
        self.domain.transport.capacity()
    }

    pub fn timer(&self) -> timer::Timer<'d> {
        self.domain.settings.timer()
    }

    pub fn config(&self) -> &config::Config {
        self.domain.config()
    }
}

/// The three waiter fanouts each retain a drain epoch across publications.
const _: () = {
    assert!(
        std::mem::size_of::<Port<'static, 'static, 'static, 'static>>()
            == 162 * std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::align_of::<Port<'static, 'static, 'static, 'static>>()
            == std::mem::align_of::<usize>()
    );
};
