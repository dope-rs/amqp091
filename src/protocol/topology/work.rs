use std::mem;

use o3::mem::rc;

use crate::failure;
use crate::failure::wire::{self, turn};
use crate::protocol::topology::records::{self, exchange, queue};
use crate::protocol::topology::store::Release as _;
use crate::protocol::topology::{self, store};
use crate::protocol::{self, method};

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
    QueueDeclare(queue::Declare<'b, 'd>, store::QueueHold<'c, 'b, 'd, ID>),
    QueueRedeclare {
        key: queue::Key<'d, ID>,
        update_current: bool,
    },
    QueueBind(
        queue::Bind<'d, ID>,
        store::Binding<'c, store::QueueBinding<'d, ID>>,
    ),
    QueueUnbind(queue::Unbind<'d, ID>),
    QueueDelete(queue::Delete<'d, ID>),
}

pub(crate) enum Failure<'c, 'b, 'd, const ID: u8> {
    Protocol(Work<'c, 'b, 'd, ID>, wire::Fault),
    Retain(Work<'c, 'b, 'd, ID>, rc::Error),
    Capacity(Work<'c, 'b, 'd, ID>, wire::Capacity),
}

pub(crate) struct Ready<'c, 'b, 'd, const ID: u8> {
    work: Work<'c, 'b, 'd, ID>,
}

impl<'c, 'b, 'd, const ID: u8> Failure<'c, 'b, 'd, ID> {
    pub(crate) fn split(self) -> (Work<'c, 'b, 'd, ID>, failure::Error) {
        match self {
            Self::Protocol(work, error) => (work, failure::Error::Protocol(error)),
            Self::Retain(work, error) => (work, failure::Error::Retain(error)),
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

    pub(crate) fn prepare<Consumer>(
        self,
        reply: &protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Ready<'c, 'b, 'd, ID>, Failure<'c, 'b, 'd, ID>> {
        use method::Kind;
        use protocol::Response;

        let work = match (self, reply) {
            (work @ Self::Qos(_), Response::Empty(Kind::Qos))
            | (work @ Self::Confirm, Response::Empty(Kind::Confirm))
            | (work @ Self::ExchangeDeclare(_, _), Response::Empty(Kind::ExchangeDeclare))
            | (work @ Self::ExchangeDelete(_), Response::Empty(Kind::ExchangeDelete))
            | (work @ Self::ExchangeBind(_, _), Response::Empty(Kind::ExchangeBind))
            | (work @ Self::ExchangeUnbind(_), Response::Empty(Kind::ExchangeUnbind))
            | (work @ Self::QueueBind(_, _), Response::Empty(Kind::QueueBind))
            | (work @ Self::QueueUnbind(_), Response::Empty(Kind::QueueUnbind))
            | (work @ Self::QueueRedeclare { .. }, Response::QueueDeclareOk(_))
            | (work @ Self::QueueDelete(_), Response::QueueDeleteOk(_)) => work,
            (Self::QueueDeclare(mut spec, plan), Response::QueueDeclareOk(ok)) => {
                if spec.user_name().is_empty() {
                    let Some(name) = ok.queue.identity() else {
                        let actual = ok.queue.as_ref().len();
                        return Err(Failure::Protocol(
                            Self::QueueDeclare(spec, plan),
                            wire::Fault::QueueIdentity { actual },
                        ));
                    };
                    let name = match name.try_clone() {
                        Ok(name) => name,
                        Err(error) => {
                            return Err(Failure::Retain(Self::QueueDeclare(spec, plan), error));
                        }
                    };
                    spec.server_name = Some(name);
                }
                Self::QueueDeclare(spec, plan)
            }
            (work, reply) => {
                let (want, shape) = work.expected();
                let cause = wire::Fault::WorkReply {
                    expected_method: want,
                    actual_method: reply.kind(),
                    expected_reply: shape,
                    actual_reply: Self::reply(reply),
                };
                return Err(Failure::Protocol(work, cause));
            }
        };
        Ok(Ready { work })
    }

    fn expected(&self) -> (method::Kind, turn::Reply) {
        let method = match self {
            Self::Qos(_) => method::Kind::Qos,
            Self::Confirm => method::Kind::Confirm,
            Self::ExchangeDeclare(_, _) => method::Kind::ExchangeDeclare,
            Self::ExchangeDelete(_) => method::Kind::ExchangeDelete,
            Self::ExchangeBind(_, _) => method::Kind::ExchangeBind,
            Self::ExchangeUnbind(_) => method::Kind::ExchangeUnbind,
            Self::QueueDeclare(_, _) | Self::QueueRedeclare { .. } => method::Kind::QueueDeclare,
            Self::QueueBind(_, _) => method::Kind::QueueBind,
            Self::QueueUnbind(_) => method::Kind::QueueUnbind,
            Self::QueueDelete(_) => method::Kind::QueueDelete,
        };
        let reply = match self {
            Self::QueueDeclare(_, _) | Self::QueueRedeclare { .. } => turn::Reply::Queue,
            Self::QueueDelete(_) => turn::Reply::Delete,
            Self::Qos(_)
            | Self::Confirm
            | Self::ExchangeDeclare(_, _)
            | Self::ExchangeDelete(_)
            | Self::ExchangeBind(_, _)
            | Self::ExchangeUnbind(_)
            | Self::QueueBind(_, _)
            | Self::QueueUnbind(_) => turn::Reply::Empty,
        };
        (method, reply)
    }

    fn reply<Consumer>(reply: &protocol::Response<'b, 'd, Consumer>) -> turn::Reply {
        match reply {
            protocol::Response::Empty(_) => turn::Reply::Empty,
            protocol::Response::QueueDeclareOk(_) => turn::Reply::Queue,
            protocol::Response::QueuePurgeOk(_) => turn::Reply::Purge,
            protocol::Response::QueueDeleteOk(_) => turn::Reply::Delete,
            protocol::Response::BasicConsumeOk(_, _) => turn::Reply::Consume,
            protocol::Response::BasicConsumeReplayOk => turn::Reply::Replay,
            protocol::Response::BasicCancelOk(_) => turn::Reply::Cancel,
        }
    }
}

impl<'c, 'b, 'd, const ID: u8> Ready<'c, 'b, 'd, ID> {
    pub(crate) fn commit(
        self,
        target: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<Option<queue::Delete<'d, ID>>, Failure<'c, 'b, 'd, ID>> {
        match self.work {
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
                let key = target.topology.queues.insert(target.values, plan, spec);
                target.local.current = Some(key);
            }
            Work::QueueRedeclare {
                key,
                update_current: set,
            } => {
                if set {
                    target.local.current = Some(key);
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
    assert!(mem::size_of::<topology::Local<'static, 0>>() == 3 * mem::size_of::<u64>());
    assert!(mem::size_of::<topology::Table<'static, 'static, 0>>() == 35 * mem::size_of::<u64>());
    assert!(
        mem::size_of::<topology::State<'_, 'static, 'static, 'static, 0>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Work<'static, 'static, 'static, 0>>() == 6 * mem::size_of::<u64>());
    assert!(
        mem::size_of::<Ready<'static, 'static, 'static, 0>>()
            == mem::size_of::<Work<'static, 'static, 'static, 0>>()
    );
    assert!(
        mem::size_of::<Option<Work<'static, 'static, 'static, 0>>>()
            == mem::size_of::<Work<'static, 'static, 'static, 0>>()
    );
};
