use o3::buffer::resident::Budget;

use crate::adapter::method::admit;
use crate::failure;
use crate::message::ack;
use crate::protocol::method::{self, basic, channel, confirm, queue, tx};
use crate::protocol::topology::records::{self, consumer};
use crate::protocol::topology::{self, need, store};
use crate::protocol::wire;
use crate::protocol::wire::schema::{self, consume, queues, resource};
use crate::protocol::wire::text::short;
use crate::state::work;

mod seal;
pub(crate) use seal::Seal;

pub enum Defer {
    Wait,
}

pub enum Plain {}
pub enum Keep {}
pub enum Tracked {}
pub enum Declaration {}

pub trait Source<'d>: method::Request + Seal + Sized {
    type Policy<const ID: u8>: admit::Admit<'d, Self, ID>;
}

pub(crate) trait Prepare<'d, const ID: u8>: method::Request {
    fn prepare<'c, 'b>(
        &self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error>;
}

pub(crate) trait Capture<'d, const ID: u8>: method::Request {
    type Schema: schema::Layout;

    fn reference(&self) -> Option<short::Name<'_>> {
        None
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        _queue: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema, <Self::Schema as schema::Layout>::Proof>
    where
        Self: method::Record<Self::Schema>,
    {
        method::Record::record(self, output)
    }

    fn capture<'c, 'b>(
        &self,
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error>;
}

impl<'d> Source<'d> for channel::Open {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for channel::Close<'_> {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for channel::CloseRef<'_> {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for tx::Select {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for tx::Commit {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for tx::Rollback {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for basic::Recover {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for queue::Inspect<'_> {
    type Policy<const ID: u8> = Tracked;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Inspect<'_> {
    type Schema = schema::Empty;

    fn reference(&self) -> Option<short::Name<'_>> {
        Some(self.queue)
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        queue: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema> {
        method::Record::record(
            &queue::Inspect {
                queue,
                durable: self.durable,
                exclusive: self.exclusive,
                auto_delete: self.auto_delete,
                arguments: self.arguments,
            },
            output,
        )
    }

    fn capture<'c, 'b>(
        &self,
        _: wire::Encoded<'_, Self::Schema>,
        _: &Budget<'d>,
        _: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        Ok(work::Admit::Direct(need::IdentityPlan::Fresh))
    }
}

impl<'d> Source<'d> for queue::Purge<'_> {
    type Policy<const ID: u8> = Tracked;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Purge<'_> {
    type Schema = schema::Empty;

    fn reference(&self) -> Option<short::Name<'_>> {
        Some(self.queue)
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        queue: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema> {
        method::Record::record(&queue::Purge { queue }, output)
    }

    fn capture<'c, 'b>(
        &self,
        _: wire::Encoded<'_, Self::Schema>,
        _: &Budget<'d>,
        _: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        Ok(work::Admit::Direct(need::IdentityPlan::None))
    }
}

impl<'d> Source<'d> for basic::Qos {
    type Policy<const ID: u8> = Keep;
}

impl<'d, const ID: u8> Prepare<'d, ID> for basic::Qos {
    fn prepare<'c, 'b>(
        &self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::none();
        let id = topo.resolve(&needs).0;
        let spec = records::Qos {
            prefetch_size: self.prefetch_size,
            prefetch_count: self.prefetch_count,
            global: self.global,
        };
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::Qos(spec),
        })
    }
}

impl<'d> Source<'d> for confirm::Select {
    type Policy<const ID: u8> = Keep;
}

impl<'d, const ID: u8> Prepare<'d, ID> for confirm::Select {
    fn prepare<'c, 'b>(
        &self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::none();
        let id = topo.resolve(&needs).0;
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::Confirm,
        })
    }
}

impl<'d, M: ack::Mode> Source<'d> for basic::Consume<'_, M> {
    type Policy<const ID: u8> = Tracked;
}

impl<'d, M: ack::Mode, const ID: u8> Capture<'d, ID> for basic::Consume<'_, M> {
    type Schema = consume::Layout;

    fn reference(&self) -> Option<short::Name<'_>> {
        Some(self.queue)
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        queue: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema, [schema::Offset; 2]> {
        method::Record::record(
            &basic::Consume {
                queue,
                consumer_tag: self.consumer_tag,
                no_local: self.no_local,
                ack: self.ack,
                exclusive: self.exclusive,
                arguments: self.arguments,
            },
            output,
        )
    }

    fn capture<'c, 'b>(
        &self,
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::reference(self.queue.into());
        let found = topo.resolve(&needs).1;
        let queue = found.queue();
        let graph = topo.graph();
        let name = found.name(self.queue, graph.current_state());
        let spec = consumer::Record::capture(budget, queue, view, name)?;
        Ok(work::Admit::Consumer(spec))
    }
}

impl<'d> Source<'d> for queue::Declare<'_> {
    type Policy<const ID: u8> = Declaration;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Declare<'_> {
    type Schema = queues::Declaration;

    fn capture<'c, 'b>(
        &self,
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::queue(self.queue.into());
        let (id, plan) = topo.preflight(&needs)?;
        let work = match plan {
            store::Upsert::Existing(key) => topology::Work::QueueRedeclare {
                key,
                update_current: true,
            },
            store::Upsert::Vacant(hold) => {
                let spec = wire::bytes::Bytes::capture(budget, view)?;
                topology::Work::QueueDeclare(spec, hold)
            }
        };
        Ok(work::Admit::Topology { identity: id, work })
    }
}

impl<'d> Source<'d> for queue::Bind<'_> {
    type Policy<const ID: u8> = Tracked;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Bind<'_> {
    type Schema = queues::Binding;

    fn reference(&self) -> Option<short::Name<'_>> {
        Some(self.queue)
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        name: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema, <Self::Schema as schema::Layout>::Proof> {
        method::Record::record(
            &queue::Bind {
                queue: name,
                exchange: self.exchange,
                routing_key: if self.queue.is_empty() && self.routing_key.is_empty() {
                    name.into()
                } else {
                    self.routing_key
                },
                arguments: self.arguments,
            },
            output,
        )
    }

    fn capture<'c, 'b>(
        &self,
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let args = view.identity();
        let needs = need::Need::binding(self.queue, self.exchange.into(), self.routing_key, args);
        let (id, plan) = topo.preflight(&needs)?;
        let found = plan.resolution();
        let spec = {
            use crate::protocol::topology::records::queue;

            let graph = topo.graph();
            let (name, route) = found.binding(self.queue, self.routing_key, graph.current_state());
            queue::Bind::capture(budget, found.queue(), view, name, route)
        };
        let (spec, choice) = topo.bind(plan.into_change(), spec)?;
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::QueueBind(spec, choice),
        })
    }
}

impl<'d> Source<'d> for queue::Unbind<'_> {
    type Policy<const ID: u8> = Tracked;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Unbind<'_> {
    type Schema = queues::Binding;

    fn reference(&self) -> Option<short::Name<'_>> {
        Some(self.queue)
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        name: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema, <Self::Schema as schema::Layout>::Proof> {
        method::Record::record(
            &queue::Unbind {
                queue: name,
                exchange: self.exchange,
                routing_key: if self.queue.is_empty() && self.routing_key.is_empty() {
                    name.into()
                } else {
                    self.routing_key
                },
                arguments: self.arguments,
            },
            output,
        )
    }

    fn capture<'c, 'b>(
        &self,
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::reference(self.queue.into());
        let (id, found) = topo.resolve(&needs);
        let spec = {
            use crate::protocol::topology::records::queue;

            let graph = topo.graph();
            let (name, route) = found.binding(self.queue, self.routing_key, graph.current_state());
            queue::Bind::capture(budget, found.queue(), view, name, route)?
        };
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::QueueUnbind(spec),
        })
    }
}

impl<'d> Source<'d> for queue::Delete<'_> {
    type Policy<const ID: u8> = Tracked;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Delete<'_> {
    type Schema = resource::Layout;

    fn reference(&self) -> Option<short::Name<'_>> {
        Some(self.queue)
    }

    fn record<'a>(
        &self,
        output: wire::outputs::Fields<'a, Self::Schema>,
        queue: short::Name<'_>,
    ) -> wire::outputs::Fields<'a, Self::Schema, [schema::Offset; 1]> {
        method::Record::record(
            &queue::Delete {
                queue,
                if_unused: self.if_unused,
                if_empty: self.if_empty,
            },
            output,
        )
    }

    fn capture<'c, 'b>(
        &self,
        _: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::reference(self.queue.into());
        let (id, found) = topo.resolve(&needs);
        let key = found.queue();
        let graph = topo.graph();
        let queue = found.name(self.queue, graph.current_state());
        let record = key.and_then(|key| graph.queue_entry(Some(key), self.queue.into()));
        let name = record
            .as_ref()
            .map_or(queue, |record| record.value().server_name());
        let spec = {
            use crate::protocol::topology::records::queue;

            queue::Delete::capture(budget, key, name)?
        };
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::QueueDelete(spec),
        })
    }
}
