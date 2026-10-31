pub(in crate::protocol::topology) mod bindings;
pub(in crate::protocol::topology) mod exchange;
pub(in crate::protocol::topology) mod head;

use o3::alloc;
use o3::depot::arena::slab;
use o3::mem::rc;

use self::bindings::Bindings;
use self::exchange::Exchanges;
use self::head::Head;
use crate::failure::wire::turn;
use crate::failure::{self, wire};
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{Plan, Record, graph, need, store};
use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::text::{resident, short};

enum Identity<'c, 'b, 'd, const ID: u8> {
    Indexed {
        record: Record<'c, 'b, 'd>,
        hash: u64,
    },
    Detached(store::Unindexed<'c, 'b, 'd, ID>),
}

pub(crate) struct Refresh<'c, 'b, 'd, const ID: u8> {
    entry: Identity<'c, 'b, 'd, ID>,
    writer: resident::Writer<'b>,
}

pub(crate) struct Local<'d, const ID: u8> {
    pub(super) head: Head,
    pub(super) current: Option<queue::Key<'d, ID>>,
}

pub(crate) struct Table<'c, 'd, const ID: u8> {
    pub(in crate::protocol::topology) queues: store::QueueStore<'c, 'd, ID>,
    pub(in crate::protocol::topology) exchanges: Exchanges<'c, 'd, ID>,
    pub(in crate::protocol::topology) bindings: Bindings<'c, 'd, ID>,
}

pub(crate) struct State<'a, 'c, 'b, 'd, const ID: u8> {
    pub(super) local: &'a mut Local<'d, ID>,
    pub(super) topology: &'a mut Table<'c, 'd, ID>,
    pub(super) values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
}

impl<const ID: u8> Local<'_, ID> {
    pub(crate) const fn new() -> Self {
        Self {
            head: Head::new(),
            current: None,
        }
    }

    pub(crate) const fn is_tracked(&self) -> bool {
        self.head.is_tracked()
    }

    pub(crate) fn clear(&mut self) {
        self.head.clear();
        self.current = None;
    }
}

impl<'c, 'd, const ID: u8> Table<'c, 'd, ID> {
    pub(crate) fn try_new(
        plan: Plan,
        permits: &crate::permits::Registry<'c, 'd>,
    ) -> Result<Self, alloc::Error> {
        let owner = store::Owner::new();
        Ok(Self {
            queues: store::QueueStore::try_new(plan.queue.index, owner, permits)?,
            exchanges: Exchanges::try_new(
                plan.exchange.slots,
                plan.exchange.index,
                owner,
                permits,
            )?,
            bindings: Bindings::try_new(
                plan.queue_bind.slots,
                plan.queue_bind.index,
                plan.exchange_bind.slots,
                plan.exchange_bind.index,
                owner,
                permits,
            )?,
        })
    }
}

impl<'a, 'c, 'b, 'd, const ID: u8> State<'a, 'c, 'b, 'd, ID> {
    pub(crate) const fn new(
        local: &'a mut Local<'d, ID>,
        topo: &'a mut Table<'c, 'd, ID>,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
    ) -> Self {
        Self {
            local,
            topology: topo,
            values,
        }
    }

    pub(crate) fn track(&mut self) {
        self.local.head.track();
    }

    pub(crate) fn clear(&mut self) {
        self.local.clear();
    }

    pub(crate) const fn graph(&self) -> graph::Graph<'_, 'c, 'b, 'd, ID> {
        graph::Graph::new(&*self.local, &*self.topology, self.values)
    }

    pub(crate) fn queue_name(
        &self,
        key: queue::Key<'d, ID>,
    ) -> Result<Option<resident::Short<'b, 'd>>, rc::Error> {
        self.values
            .entry(key.into_parts().0.parts())
            .map(|entry| entry.value().try_name())
            .transpose()
    }

    pub(crate) fn redeclare(
        &self,
        key: queue::Key<'d, ID>,
    ) -> Result<Record<'c, 'b, 'd>, wire::Fault> {
        self.values
            .entry(key.into_parts().0.parts())
            .ok_or(wire::Fault::TopologyQueue {
                queue: key.identity(),
            })
    }

    pub(crate) fn refresh(
        &mut self,
        mut record: Record<'c, 'b, 'd>,
        ids: &'b resident::Pool,
    ) -> Result<Refresh<'c, 'b, 'd, ID>, failure::Error> {
        let key = queue::Key::from_parts(record.key(), self.topology.queues.owner);
        let failure = |actual| {
            failure::Error::Protocol(wire::Fault::TopologyIdentity {
                queue: key.identity(),
                expected: turn::Identity::Server,
                actual,
            })
        };
        let queue = record.value_mut();
        if !queue.user_name().is_empty() {
            return Err(failure(turn::Identity::User));
        }
        let id = queue
            .server_name
            .take()
            .ok_or_else(|| failure(turn::Identity::Vacant))?;
        match id.try_writer() {
            Ok(writer) => {
                let entry = self.topology.queues.detach(record, writer.hash());
                Ok(Refresh {
                    entry: Identity::Detached(entry),
                    writer,
                })
            }
            Err(id) => {
                let hash = id.hash();
                queue.server_name = Some(id);
                let writer = ids
                    .try_writer()
                    .ok_or(failure::Error::Capacity(wire::Capacity::TopologyIdentity))?;
                Ok(Refresh {
                    entry: Identity::Indexed { record, hash },
                    writer,
                })
            }
        }
    }

    pub(super) fn forget_queue(
        &mut self,
        spec: queue::Delete<'d, ID>,
    ) -> Option<queue::Delete<'d, ID>> {
        let key = spec.queue_key();
        if let Some(key) = key {
            let entry = self.values.entry(key.into_parts().0.parts())?;
            let hash = Fingerprint::of(entry.value().server_name().value().as_bytes()).get();
            self.topology.queues.detach(entry, hash).discard();
            if self.local.current == Some(key) {
                self.local.current = None;
            }
        }
        self.topology.bindings.forget_queue(key, spec.name());
        Some(spec)
    }

    pub(super) fn forget_route(&mut self, name: short::Short<'_>) {
        self.topology.exchanges.forget(name);
        self.topology.bindings.forget_route(name);
    }

    pub(super) fn remove_queue(&mut self, spec: &queue::Unbind<'d, ID>) {
        let record = match spec.key() {
            Some(key) => {
                let Some(record) = self.values.entry(key.into_parts().0.parts()) else {
                    return;
                };
                Some(record)
            }
            None => None,
        };
        self.topology.bindings.remove_queue(spec, record.as_ref());
    }

    pub(crate) fn bind<T, P: store::Release<'c, 'd, ID>>(
        &mut self,
        plan: P,
        result: Result<T, failure::Error>,
    ) -> Result<(T, P), failure::Error> {
        match result {
            Ok(value) => Ok((value, plan)),
            Err(error) => {
                plan.release(self.topology);
                Err(error)
            }
        }
    }

    pub(crate) fn preflight<M: need::Reserve<'c, 'b, 'd, ID>>(
        &mut self,
        needs: &need::Need<'_, M>,
    ) -> Result<(need::IdentityPlan, M::Plan), failure::Error> {
        let (id, found) = self.resolve(needs);
        let plan = needs.reserve(self, found)?;
        Ok((id, plan))
    }

    pub(crate) fn resolve<M>(
        &mut self,
        needs: &need::Need<'_, M>,
    ) -> (need::IdentityPlan, need::Resolution<'d, ID>) {
        let found = match needs.target() {
            need::Target::None => None,
            need::Target::Reference(name) => self.graph().queue_key(name),
            need::Target::Declare(name) => (!name.is_empty())
                .then(|| self.graph().queue_key(name))
                .flatten(),
        };
        let found = need::Resolution::new(found);
        (needs.identity(), found)
    }
}

impl<'c, 'b, 'd, const ID: u8> Refresh<'c, 'b, 'd, ID> {
    pub(crate) fn phase(&self) -> turn::Flight {
        match self.entry {
            Identity::Indexed { .. } => turn::Flight::Identity,
            Identity::Detached(_) => turn::Flight::IdentityReuse,
        }
    }

    pub(crate) fn commit(
        self,
        target: &mut State<'_, 'c, 'b, 'd, ID>,
        value: short::Name<'_>,
    ) -> (queue::Key<'d, ID>, resident::Short<'b, 'd>) {
        let mut entry = match self.entry {
            Identity::Indexed { record, hash } => target.topology.queues.detach(record, hash),
            Identity::Detached(entry) => entry,
        };
        let key = entry.key();
        let (stored, name) = self.writer.commit_pair(value.into());
        let hash = stored.hash();
        entry.value_mut().server_name = Some(stored);
        drop(target.topology.queues.index(entry, hash));
        (key, name.into())
    }

    pub(crate) fn undo(self, target: &mut State<'_, 'c, 'b, 'd, ID>) {
        if let Identity::Detached(mut entry) = self.entry {
            let id = self.writer.restore();
            let hash = id.hash();
            entry.value_mut().server_name = Some(id);
            drop(target.topology.queues.index(entry, hash));
        }
    }
}
