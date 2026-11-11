use std::mem;

use crate::failure;
use crate::failure::wire;
use crate::protocol::topology::records::{self, exchange, queue};
use crate::protocol::topology::store::Release as _;
use crate::protocol::topology::{self, store};
use crate::protocol::wire::{bytes, schema::queues};

pub(crate) enum Work<'c, 'b, 'd, const ID: u8> {
    Qos(records::Qos),
    Confirm,
    ExchangeDeclare(
        exchange::Declare<'d>,
        store::Binding<'c, store::Exchange<'d, ID>>,
    ),
    ExchangeDelete(exchange::Delete<'d>),
    ExchangeBind(exchange::Bind<'d>, store::Binding<'c, store::Link<'d, ID>>),
    ExchangeUnbind(exchange::Bind<'d>),
    QueueDeclare(
        bytes::Bytes<'d, queues::Declaration>,
        store::QueueHold<'c, 'b, 'd, ID>,
    ),
    QueueRedeclare {
        key: queue::Key<'d, ID>,
        update_current: bool,
    },
    QueueBind(
        queue::Bind<'d, ID>,
        store::Binding<'c, store::QueueBinding<'d, ID>>,
    ),
    QueueUnbind(queue::Bind<'d, ID>),
    QueueDelete(queue::Delete<'d, ID>),
}

pub(crate) enum Failure<'c, 'b, 'd, const ID: u8> {
    Protocol(Work<'c, 'b, 'd, ID>, wire::Fault),
    Capacity(Work<'c, 'b, 'd, ID>, wire::Capacity),
}

impl<'c, 'b, 'd, const ID: u8> Failure<'c, 'b, 'd, ID> {
    pub(crate) fn split(self) -> (Work<'c, 'b, 'd, ID>, failure::Error) {
        match self {
            Self::Protocol(work, error) => (work, failure::Error::Protocol(error)),
            Self::Capacity(work, error) => (work, failure::Error::Capacity(error)),
        }
    }
}

impl<'c, 'b, 'd, const ID: u8> Work<'c, 'b, 'd, ID> {
    pub(crate) fn undo(self, target: &mut topology::State<'_, 'c, 'b, 'd, ID>) {
        match self {
            Self::ExchangeDeclare(_, plan) => plan.release(target.topology),
            Self::ExchangeBind(_, plan) => plan.release(target.topology),
            Self::QueueDeclare(_, plan) => drop(plan),
            Self::QueueBind(_, plan) => plan.release(target.topology),
            Self::Qos(_)
            | Self::Confirm
            | Self::ExchangeDelete(_)
            | Self::ExchangeUnbind(_)
            | Self::QueueRedeclare { .. }
            | Self::QueueUnbind(_)
            | Self::QueueDelete(_) => {}
        }
    }

    pub(crate) fn commit(
        self,
        target: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<Option<queue::Delete<'d, ID>>, Failure<'c, 'b, 'd, ID>> {
        match self {
            Work::Qos(spec) => {
                target.local.head.qos = Some(spec);
            }
            Work::Confirm => {
                target.local.head.confirms = true;
            }
            Work::ExchangeDeclare(spec, plan) => {
                target
                    .topology
                    .exchanges
                    .record(spec, plan)
                    .map_err(|(spec, plan, error)| {
                        Failure::Capacity(Work::ExchangeDeclare(spec, plan), error)
                    })?;
            }
            Work::ExchangeDelete(name) => {
                target.forget_route(name.name());
            }
            Work::ExchangeBind(spec, plan) => {
                target.topology.bindings.record_route(spec, plan).map_err(
                    |(spec, plan, error)| Failure::Capacity(Work::ExchangeBind(spec, plan), error),
                )?;
            }
            Work::ExchangeUnbind(spec) => {
                target.topology.bindings.remove_route(&spec);
            }
            Work::QueueDeclare(spec, plan) => {
                target.declare(spec, plan, None);
            }
            Work::QueueRedeclare {
                key,
                update_current: set,
            } => {
                if set {
                    target.local.current = queue::Current::Tracked(key);
                }
            }
            Work::QueueBind(spec, plan) => {
                if let Some(key) = spec.key()
                    && target.values.entry(key.into_parts().0.parts()).is_none()
                {
                    return Err(Failure::Protocol(
                        Work::QueueBind(spec, plan),
                        wire::Fault::TopologyQueue {
                            queue: key.identity(),
                        },
                    ));
                }
                target.topology.bindings.record_queue(spec, plan).map_err(
                    |(spec, plan, error)| Failure::Capacity(Work::QueueBind(spec, plan), error),
                )?;
            }
            Work::QueueUnbind(spec) => {
                target.remove_queue(&spec);
            }
            Work::QueueDelete(name) => {
                return Ok(target.forget_queue(name));
            }
        }
        Ok(None)
    }
}

const _: () = {
    assert!(mem::size_of::<topology::Local<'static, 0>>() <= 20 * mem::size_of::<u64>());
    assert!(mem::size_of::<topology::Table<'static, 'static, 0>>() == 32 * mem::size_of::<u64>());
    assert!(
        mem::size_of::<topology::State<'_, 'static, 'static, 'static, 0>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Work<'static, 'static, 'static, 0>>() == 6 * mem::size_of::<u64>());
    assert!(
        mem::size_of::<Option<Work<'static, 'static, 'static, 0>>>()
            == mem::size_of::<Work<'static, 'static, 'static, 0>>()
    );
};
