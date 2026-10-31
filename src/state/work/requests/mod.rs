use o3::buffer::resident::Budget;

use crate::adapter::method::admit;
use crate::failure;
use crate::message::ack;
use crate::protocol::method::{self, basic, channel, confirm, queue, tx};
use crate::protocol::topology::records::{self, consumer};
use crate::protocol::topology::{self, need, store};
use crate::protocol::wire;
use crate::protocol::wire::schema::{self, consume, queues, resource};
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
    const RETAIN: bool = false;
}

pub(crate) trait Prepare<'d, const ID: u8>: method::Request {
    fn prepare<'c, 'b>(
        &self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error>;
}

pub(crate) trait Capture<'d, const ID: u8>: method::Request {
    type Schema: schema::Layout;

    fn capture<'c, 'b>(
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

impl<'d> Source<'d> for basic::CancelRef<'_> {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for basic::Recover {
    type Policy<const ID: u8> = Plain;
}

impl<'d> Source<'d> for queue::Inspect<'_> {
    type Policy<const ID: u8> = Plain;
    const RETAIN: bool = true;
}

impl<'d> Source<'d> for queue::Purge<'_> {
    type Policy<const ID: u8> = Plain;
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

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let queue = view.field(consume::Layout::QUEUE);
        let needs = need::Need::reference(queue);
        let found = topo.resolve(&needs).1;
        let queue = found.queue();
        let spec = consumer::Record::capture(budget, queue, view)?;
        Ok(work::Admit::Consumer(spec))
    }
}

impl<'d> Source<'d> for queue::Declare<'_> {
    type Policy<const ID: u8> = Declaration;
}

impl<'d, const ID: u8> Capture<'d, ID> for queue::Declare<'_> {
    type Schema = queues::Declaration;

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let name = view.field(queues::Declaration::QUEUE);
        let needs = need::Need::queue(name);
        let (id, plan) = topo.preflight(&needs)?;
        let work = match plan {
            store::Upsert::Existing(key) => topology::Work::QueueRedeclare {
                key,
                update_current: true,
            },
            store::Upsert::Vacant(hold) => {
                use crate::protocol::topology::records::queue;

                let spec = queue::Declare::capture(budget, view)?;
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

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let queue = view.field(queues::Binding::QUEUE);
        let ex = view.field(queues::Binding::EXCHANGE);
        let route = view.field(queues::Binding::ROUTING_KEY);
        let args = view.identity();
        let needs = need::Need::binding(queue, ex, route, args);
        let (id, plan) = topo.preflight(&needs)?;
        let queue = plan.resolution().queue();
        let (spec, choice) = topo.bind(plan.into_change(), {
            use crate::protocol::topology::records::queue;

            queue::Bind::capture(budget, queue, view)
        })?;
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

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let queue = view.field(queues::Binding::QUEUE);
        let needs = need::Need::reference(queue);
        let (id, found) = topo.resolve(&needs);
        let queue = found.queue();
        let spec = {
            use crate::protocol::topology::records::queue;

            queue::Unbind::capture(budget, queue, view)?
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

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let queue = view.field(resource::Layout::NAME);
        let needs = need::Need::reference(queue);
        let (id, found) = topo.resolve(&needs);
        let key = found.queue();
        let record = key.and_then(|key| topo.graph().queue_entry(Some(key), queue));
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
