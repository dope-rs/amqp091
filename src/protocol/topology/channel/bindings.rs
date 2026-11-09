use o3::alloc;
use o3::depot::{capacity, map};

use crate::failure::wire::Capacity;
use crate::protocol::topology::records::{exchange, queue};
use crate::protocol::topology::{Record, need, store};
use crate::protocol::wire;
use crate::protocol::wire::text::short;

pub(in crate::protocol::topology) struct Bindings<'c, 'd, const ID: u8> {
    pub(in crate::protocol::topology) queues:
        store::Store<'c, queue::Bind<'d, ID>, store::QueueBinding<'d, ID>>,
    pub(in crate::protocol::topology) exchanges:
        store::Store<'c, exchange::Bind<'d>, store::Link<'d, ID>>,
}

impl<'c, 'd, const ID: u8> Bindings<'c, 'd, ID> {
    pub(super) fn try_new(
        q_cap: capacity::Count,
        q_keys: map::Plan,
        x_cap: capacity::Count,
        x_keys: map::Plan,
        permits: &crate::permits::Registry<'c, 'd>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            queues: store::Store::try_new(q_cap, q_keys, permits)?,
            exchanges: store::Store::try_new(x_cap, x_keys, permits)?,
        })
    }

    pub(in crate::protocol::topology) fn queue_key_at(
        &self,
        hash: u64,
        queue: Option<queue::Key<'d, ID>>,
        name: short::Short<'_>,
        source: short::Short<'_>,
        route: Option<short::Short<'_>>,
        args: &[u8],
    ) -> Option<store::QueueBinding<'d, ID>> {
        self.queues
            .key(hash, |item| item.matches(queue, name, source, route, args))
    }

    pub(in crate::protocol::topology) fn exchange_at(
        &self,
        hash: u64,
        dest: short::Short<'_>,
        source: short::Short<'_>,
        route: short::Short<'_>,
        args: &[u8],
    ) -> Option<store::Link<'d, ID>> {
        self.exchanges
            .key(hash, |item| item.matches(dest, source, route, args))
    }

    pub(in crate::protocol::topology) fn record_queue(
        &mut self,
        spec: queue::Bind<'d, ID>,
        plan: store::Binding<'c, store::QueueBinding<'d, ID>>,
    ) -> Result<
        (),
        (
            queue::Bind<'d, ID>,
            store::Binding<'c, store::QueueBinding<'d, ID>>,
            Capacity,
        ),
    > {
        self.queues.upsert(plan, spec, |left, right| {
            left.matches(
                right.key(),
                right.name(),
                right.exchange(),
                right.routing_key(),
                right.arguments(),
            )
        })
    }

    pub(in crate::protocol::topology) fn record_route(
        &mut self,
        spec: exchange::Bind<'d>,
        plan: store::Binding<'c, store::Link<'d, ID>>,
    ) -> Result<
        (),
        (
            exchange::Bind<'d>,
            store::Binding<'c, store::Link<'d, ID>>,
            Capacity,
        ),
    > {
        self.exchanges.upsert(plan, spec, |left, right| {
            left.matches(
                right.destination(),
                right.source(),
                right.routing_key(),
                right.arguments(),
            )
        })
    }

    pub(super) fn remove_queue(
        &mut self,
        spec: &queue::Bind<'d, ID>,
        record: Option<&Record<'c, '_, 'd>>,
    ) {
        let args = wire::Identity::of(spec.arguments());
        let route = spec.routing_key();
        let current = record.map(|record| short::Short::from(record.value().server_name()));
        let alias = match (route, current) {
            (None, Some(name)) => Some(Some(name)),
            (Some(route), Some(name)) if route == name => Some(None),
            _ => None,
        };
        for route in std::iter::once(route).chain(alias) {
            for (target, name) in
                std::iter::once((spec.key(), spec.name())).chain(current.map(|name| (None, name)))
            {
                let hash =
                    need::Binding::hash(target, name, spec.exchange(), route, args.fingerprint());
                self.queues.remove(
                    hash,
                    |item| item.matches(target, name, spec.exchange(), route, args.bytes()),
                    |spec| {
                        need::Binding::hash(
                            spec.key(),
                            spec.name(),
                            spec.exchange(),
                            spec.routing_key(),
                            wire::Fingerprint::of(spec.arguments()),
                        )
                    },
                );
            }
        }
    }

    pub(in crate::protocol::topology) fn remove_route(&mut self, spec: &exchange::Bind<'d>) {
        let args = wire::Identity::of(spec.arguments());
        let hash = need::Route::hash(
            spec.destination(),
            spec.source(),
            spec.routing_key(),
            args.fingerprint(),
        );
        self.exchanges.remove(
            hash,
            |item| {
                item.matches(
                    spec.destination(),
                    spec.source(),
                    spec.routing_key(),
                    args.bytes(),
                )
            },
            |spec| {
                need::Route::hash(
                    spec.destination(),
                    spec.source(),
                    spec.routing_key(),
                    wire::Fingerprint::of(spec.arguments()),
                )
            },
        );
    }

    pub(super) fn forget_queue(&mut self, key: Option<queue::Key<'d, ID>>, name: short::Short<'_>) {
        self.queues.remove_where(
            |spec| match spec.key() {
                Some(stored) => Some(stored) == key,
                None => spec.name() == name,
            },
            |spec| {
                need::Binding::hash(
                    spec.key(),
                    spec.name(),
                    spec.exchange(),
                    spec.routing_key(),
                    wire::Fingerprint::of(spec.arguments()),
                )
            },
        );
    }

    pub(super) fn forget_route(&mut self, name: short::Short<'_>) {
        self.queues.remove_where(
            |spec| spec.exchange().value() == name.value(),
            |spec| {
                need::Binding::hash(
                    spec.key(),
                    spec.name(),
                    spec.exchange(),
                    spec.routing_key(),
                    wire::Fingerprint::of(spec.arguments()),
                )
            },
        );
        self.exchanges.remove_where(
            |spec| {
                spec.source().value() == name.value() || spec.destination().value() == name.value()
            },
            |spec| {
                need::Route::hash(
                    spec.destination(),
                    spec.source(),
                    spec.routing_key(),
                    wire::Fingerprint::of(spec.arguments()),
                )
            },
        );
    }
}
