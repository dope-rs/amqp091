use std::mem;

use crate::failure;
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{State, store};
use crate::protocol::wire;
use crate::protocol::wire::text::short;

pub(crate) struct Need<'a, M> {
    change: M,
    target: Target<'a>,
    identity: IdentityPlan,
}

pub(crate) struct Plan<'c, 'd, const ID: u8> {
    found: Resolution<'d, ID>,
    change: store::Binding<'c, store::QueueBinding<'d, ID>>,
}

pub(crate) struct Exchange<'a> {
    name: short::Short<'a>,
}

pub(crate) struct Route<'a> {
    dest: short::Short<'a>,
    source: short::Short<'a>,
    key: short::Short<'a>,
    args: wire::Identity<'a>,
}

pub(crate) struct Queue<'a> {
    name: short::Short<'a>,
}

pub(crate) struct Binding<'a> {
    queue: short::Name<'a>,
    source: short::Short<'a>,
    key: short::Short<'a>,
    args: wire::Identity<'a>,
}

#[derive(Clone, Copy)]
pub(crate) enum IdentityPlan {
    None,
    Fresh,
}

#[derive(Clone, Copy)]
pub(in crate::protocol::topology) enum Target<'a> {
    None,
    Reference(short::Short<'a>),
    Declare(short::Short<'a>),
}

#[derive(Clone, Copy)]
pub(crate) struct Resolution<'d, const ID: u8> {
    queue: Option<queue::Key<'d, ID>>,
}

pub(crate) trait Reserve<'c, 'b, 'd, const ID: u8> {
    type Plan;

    fn reserve(
        &self,
        state: &mut State<'_, 'c, 'b, 'd, ID>,
        found: Resolution<'d, ID>,
    ) -> Result<Self::Plan, failure::Error>;
}

fn write_short(hash: &mut wire::Fingerprint, value: short::Short<'_>) {
    hash.write(&[value.len()]);
    hash.write(value.value().as_bytes());
}

impl<'d, const ID: u8> Resolution<'d, ID> {
    pub(in crate::protocol::topology) const fn new(queue: Option<queue::Key<'d, ID>>) -> Self {
        Self { queue }
    }

    pub(crate) const fn queue(self) -> Option<queue::Key<'d, ID>> {
        self.queue
    }

    pub(crate) fn name<'a>(
        self,
        input: short::Name<'a>,
        current: &'a queue::Current<'d, ID>,
    ) -> short::Name<'a> {
        if self.queue.is_none() && input.is_empty() {
            current.name().unwrap_or(input)
        } else {
            input
        }
    }

    pub(crate) fn binding<'a>(
        self,
        input: short::Name<'a>,
        route: short::Short<'a>,
        current: &'a queue::Current<'d, ID>,
    ) -> (short::Name<'a>, short::Short<'a>) {
        let name = self.name(input, current);
        let route = if input.is_empty() && route.is_empty() {
            name.into()
        } else {
            route
        };
        (name, route)
    }
}

impl<'c, 'd, const ID: u8> Plan<'c, 'd, ID> {
    fn new(
        found: Resolution<'d, ID>,
        change: store::Binding<'c, store::QueueBinding<'d, ID>>,
    ) -> Self {
        Self { found, change }
    }

    pub(crate) const fn resolution(&self) -> Resolution<'d, ID> {
        self.found
    }

    pub(crate) fn into_change(self) -> store::Binding<'c, store::QueueBinding<'d, ID>> {
        self.change
    }
}

impl<'a> Need<'a, ()> {
    pub(crate) const fn none() -> Self {
        Self {
            change: (),
            target: Target::None,
            identity: IdentityPlan::None,
        }
    }

    pub(crate) const fn reference(queue: short::Short<'a>) -> Self {
        Self {
            change: (),
            target: Target::Reference(queue),
            identity: IdentityPlan::None,
        }
    }
}

impl<'a> Need<'a, Exchange<'a>> {
    pub(crate) const fn exchange(name: short::Short<'a>) -> Self {
        Self {
            change: Exchange { name },
            target: Target::None,
            identity: IdentityPlan::None,
        }
    }
}

impl<'a> Need<'a, Route<'a>> {
    pub(crate) const fn route(
        dest: short::Short<'a>,
        source: short::Short<'a>,
        key: short::Short<'a>,
        args: wire::Identity<'a>,
    ) -> Self {
        Self {
            change: Route {
                dest,
                source,
                key,
                args,
            },
            target: Target::None,
            identity: IdentityPlan::None,
        }
    }
}

impl<'a> Need<'a, Queue<'a>> {
    pub(crate) fn queue(name: short::Short<'a>) -> Self {
        Self {
            change: Queue { name },
            target: Target::Declare(name),
            identity: if name.is_empty() {
                IdentityPlan::Fresh
            } else {
                IdentityPlan::None
            },
        }
    }
}

impl<'a> Need<'a, Binding<'a>> {
    pub(crate) fn binding(
        queue: short::Name<'a>,
        source: short::Short<'a>,
        key: short::Short<'a>,
        args: wire::Identity<'a>,
    ) -> Self {
        Self {
            change: Binding {
                queue,
                source,
                key,
                args,
            },
            target: Target::Reference(queue.into()),
            identity: IdentityPlan::None,
        }
    }
}

impl<'a, M> Need<'a, M> {
    pub(in crate::protocol::topology) const fn target(&self) -> Target<'a> {
        self.target
    }

    pub(in crate::protocol::topology) const fn identity(&self) -> IdentityPlan {
        self.identity
    }
}

impl<M> Need<'_, M> {
    pub(in crate::protocol::topology) fn reserve<'c, 'b, 'd, const ID: u8>(
        &self,
        state: &mut State<'_, 'c, 'b, 'd, ID>,
        found: Resolution<'d, ID>,
    ) -> Result<M::Plan, failure::Error>
    where
        M: Reserve<'c, 'b, 'd, ID>,
    {
        self.change.reserve(state, found)
    }
}

impl<'c, 'b: 'c, 'd: 'c, const ID: u8> Reserve<'c, 'b, 'd, ID> for Exchange<'_> {
    type Plan = store::Binding<'c, store::Exchange<'d, ID>>;

    fn reserve(
        &self,
        state: &mut State<'_, 'c, 'b, 'd, ID>,
        _: Resolution<'d, ID>,
    ) -> Result<Self::Plan, failure::Error> {
        let table = &mut *state.topology;
        let hash = wire::Fingerprint::of(self.name.value().as_bytes()).get();
        let entry = if let Some(key) = table.exchanges.key(hash, self.name) {
            store::Upsert::Existing(key)
        } else {
            store::Upsert::Vacant(
                table
                    .exchanges
                    .store
                    .reserve()
                    .map_err(failure::Error::Capacity)?,
            )
        };
        Ok(store::Binding { hash, entry })
    }
}

impl Route<'_> {
    pub(in crate::protocol::topology) fn hash(
        dest: short::Short<'_>,
        source: short::Short<'_>,
        key: short::Short<'_>,
        args: wire::Fingerprint,
    ) -> u64 {
        let mut hash = wire::Fingerprint::new();
        write_short(&mut hash, dest);
        write_short(&mut hash, source);
        write_short(&mut hash, key);
        hash.write(&args.get().to_le_bytes());
        hash.get()
    }
}

impl<'c, 'b: 'c, 'd: 'c, const ID: u8> Reserve<'c, 'b, 'd, ID> for Route<'_> {
    type Plan = store::Binding<'c, store::Link<'d, ID>>;

    fn reserve(
        &self,
        state: &mut State<'_, 'c, 'b, 'd, ID>,
        _: Resolution<'d, ID>,
    ) -> Result<Self::Plan, failure::Error> {
        let table = &mut *state.topology;
        let hash = Self::hash(self.dest, self.source, self.key, self.args.fingerprint());
        let entry = if let Some(key) =
            table
                .bindings
                .exchange_at(hash, self.dest, self.source, self.key, self.args.bytes())
        {
            store::Upsert::Existing(key)
        } else {
            store::Upsert::Vacant(
                table
                    .bindings
                    .exchanges
                    .reserve()
                    .map_err(failure::Error::Capacity)?,
            )
        };
        Ok(store::Binding { hash, entry })
    }
}

impl<'c, 'b: 'c, 'd: 'c, const ID: u8> Reserve<'c, 'b, 'd, ID> for Queue<'_> {
    type Plan = store::Upsert<queue::Key<'d, ID>, store::QueueHold<'c, 'b, 'd, ID>>;

    fn reserve(
        &self,
        state: &mut State<'_, 'c, 'b, 'd, ID>,
        found: Resolution<'d, ID>,
    ) -> Result<Self::Plan, failure::Error> {
        let table = &mut *state.topology;
        if !self.name.is_empty()
            && let Some(key) = found.queue()
        {
            Ok(store::Upsert::Existing(key))
        } else {
            table
                .queues
                .reserve(state.values)
                .map(store::Upsert::Vacant)
        }
    }
}

impl Binding<'_> {
    pub(in crate::protocol::topology) fn hash<'d, const ID: u8>(
        queue: Option<queue::Key<'d, ID>>,
        name: short::Short<'_>,
        source: short::Short<'_>,
        key: Option<short::Short<'_>>,
        args: wire::Fingerprint,
    ) -> u64 {
        let mut hash = wire::Fingerprint::new();
        match queue {
            Some(key) => {
                hash.write(&[1, 1]);
                hash.write(&key.index().to_le_bytes());
                hash.write(&key.generation().get().to_le_bytes());
            }
            None => {
                hash.write(&[0]);
                write_short(&mut hash, name);
            }
        }
        write_short(&mut hash, source);
        if let Some(key) = key {
            write_short(&mut hash, key);
        }
        hash.write(&args.get().to_le_bytes());
        hash.get()
    }
}

impl<'c, 'b: 'c, 'd: 'c, const ID: u8> Reserve<'c, 'b, 'd, ID> for Binding<'_> {
    type Plan = Plan<'c, 'd, ID>;

    fn reserve(
        &self,
        state: &mut State<'_, 'c, 'b, 'd, ID>,
        found: Resolution<'d, ID>,
    ) -> Result<Self::Plan, failure::Error> {
        let queue = found.queue();
        let (name, route) = found.binding(self.queue, self.key, &state.local.current);
        let route = queue::routing_key(name.into(), route);
        let table = &mut *state.topology;
        let hash = Self::hash(
            queue,
            name.into(),
            self.source,
            route,
            self.args.fingerprint(),
        );
        let entry = if let Some(key) = table.bindings.queue_key_at(
            hash,
            queue,
            name.into(),
            self.source,
            route,
            self.args.bytes(),
        ) {
            store::Upsert::Existing(key)
        } else {
            let hold = table
                .bindings
                .queues
                .reserve()
                .map_err(failure::Error::Capacity)?;
            store::Upsert::Vacant(hold)
        };
        Ok(Plan::new(found, store::Binding { hash, entry }))
    }
}

const _: () = assert!(mem::size_of::<Need<'_, ()>>() == 4 * mem::size_of::<usize>());
