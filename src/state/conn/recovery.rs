use std::{mem, num, ops};

use replays::Step;

use crate::failure;
use crate::failure::wire::{Fault, Replay};
use crate::protocol::replays;
use crate::protocol::topology::records::queue;
use crate::protocol::wire::text::resident::Pool;
use crate::state::channel::consumer;
use crate::state::shared::routes::Edit;
use crate::state::{channels, epoch};

#[derive(Clone, Copy)]
enum Node<'c, 'd, const ID: u8> {
    Open {
        after: Option<channels::Index<'c, ID>>,
        root: Option<channels::Lease<'d, ID>>,
    },
    Confirm {
        after: channels::Index<'c, ID>,
        lease: channels::Lease<'d, ID>,
        root: channels::Lease<'d, ID>,
    },
    Qos {
        after: channels::Index<'c, ID>,
        lease: channels::Lease<'d, ID>,
        root: channels::Lease<'d, ID>,
    },
    Exchange {
        index: usize,
        root: channels::Lease<'d, ID>,
    },
    Queue {
        after: Option<queue::Cursor<'d, ID>>,
        root: channels::Lease<'d, ID>,
    },
    QueueBind {
        index: usize,
        root: channels::Lease<'d, ID>,
    },
    ExchangeBind {
        index: usize,
        root: channels::Lease<'d, ID>,
    },
    Current {
        after: Option<channels::Index<'c, ID>>,
        root: channels::Lease<'d, ID>,
    },
    Reopen {
        after: channels::Index<'c, ID>,
        root: channels::Lease<'d, ID>,
    },
    QueueCurrent {
        after: channels::Index<'c, ID>,
        lease: channels::Lease<'d, ID>,
        root: channels::Lease<'d, ID>,
    },
    Consumer {
        after: channels::Index<'c, ID>,
        lease: channels::Lease<'d, ID>,
        item: Option<usize>,
        root: channels::Lease<'d, ID>,
    },
    Done,
}

pub(crate) struct Ticket<'c, 'd, const ID: u8> {
    target: epoch::Id<'d, ID>,
    lease: channels::Lease<'d, ID>,
    next: Node<'c, 'd, ID>,
    intent: replays::Action<'d, consumer::Key, ID>,
}

pub(crate) enum Plan<'d, R, const ID: u8> {
    Idle,
    Ready(epoch::Id<'d, ID>),
    Item(R),
}

#[derive(Clone, Copy)]
enum Status<'c, 'd, const ID: u8> {
    Idle,
    Runnable {
        target: epoch::Id<'d, ID>,
        node: Node<'c, 'd, ID>,
    },
    Waiting {
        target: epoch::Id<'d, ID>,
        lease: channels::Lease<'d, ID>,
        node: Node<'c, 'd, ID>,
    },
}

pub(crate) struct Recovery<'c, 'd, const ID: u8> {
    status: Status<'c, 'd, ID>,
}

impl<'c, 'd, const ID: u8> Ticket<'c, 'd, ID> {
    pub(crate) const fn lease(&self) -> channels::Lease<'d, ID> {
        self.lease
    }
}

impl<'c, 'd, const ID: u8> Recovery<'c, 'd, ID> {
    pub(crate) const fn new() -> Self {
        Self {
            status: Status::Idle,
        }
    }

    pub(crate) fn start(&mut self, target: epoch::Id<'d, ID>) {
        self.status = Status::Runnable {
            target,
            node: Node::Open {
                after: None,
                root: None,
            },
        };
    }

    pub(crate) fn reset(&mut self) {
        self.status = Status::Idle;
    }

    pub(crate) const fn target(&self) -> Option<epoch::Id<'d, ID>> {
        match self.status {
            Status::Idle => None,
            Status::Runnable { target, .. } | Status::Waiting { target, .. } => Some(target),
        }
    }

    pub(crate) const fn runnable(&self) -> Option<epoch::Id<'d, ID>> {
        match self.status {
            Status::Runnable { target, .. } => Some(target),
            Status::Idle | Status::Waiting { .. } => None,
        }
    }

    fn missing(stage: Replay, lease: channels::Lease<'d, ID>) -> failure::Error {
        failure::Error::Protocol(Fault::ReplayRoute {
            stage,
            channel: lease.id(),
            identity: num::NonZero::from(lease),
        })
    }

    /// Completes the one-shot admission whose caller retains exclusive region
    /// access from planning through transport reservation and this transition.
    pub(crate) fn commit<'r, 'b>(
        &mut self,
        ticket: Ticket<'c, 'd, ID>,
        routes: &mut Edit<'_, 'r, 'c, 'b, 'd, ID>,
        ids: &'b Pool,
    ) -> Result<(), failure::Error> {
        routes.try_project(ticket.lease, |channel, topology| {
            channel.method_state().replay(ticket.intent, topology, ids)
        })??;
        self.status = Status::Waiting {
            target: ticket.target,
            lease: ticket.lease,
            node: ticket.next,
        };
        Ok(())
    }

    pub(crate) fn resume(&mut self, lease: channels::Lease<'d, ID>) -> Result<(), failure::Error> {
        match self.status {
            Status::Waiting {
                target,
                lease: expected,
                node,
            } if expected == lease => {
                self.status = Status::Runnable { target, node };
                Ok(())
            }
            Status::Waiting {
                lease: expected, ..
            } => Err(failure::Error::Protocol(Fault::ReplayResponse {
                expected: Some(num::NonZero::from(expected)),
                actual: num::NonZero::from(lease),
            })),
            Status::Idle | Status::Runnable { .. } => {
                Err(failure::Error::Protocol(Fault::ReplayResponse {
                    expected: None,
                    actual: num::NonZero::from(lease),
                }))
            }
        }
    }

    pub(crate) fn plan<'r, 'b, R>(
        &mut self,
        routes: &mut Edit<'_, 'r, 'c, 'b, 'd, ID>,
        mut visit: impl for<'a> FnMut(Step<'a, 'b, 'd, consumer::Key, ID>, Ticket<'c, 'd, ID>) -> R,
    ) -> Result<Plan<'d, R, ID>, failure::Error> {
        let (target, node) = match &mut self.status {
            Status::Runnable { target, node } => (*target, node),
            Status::Idle | Status::Waiting { .. } => return Ok(Plan::Idle),
        };
        loop {
            match *node {
                Node::Open { after, root } => {
                    let Some((cursor, lease)) = routes.next_tracked(after) else {
                        *node = match root {
                            Some(root) => Node::Exchange { index: 0, root },
                            None => Node::Done,
                        };
                        continue;
                    };
                    let preference = |current: &queue::Current<'d, ID>| match current {
                        queue::Current::None => 0,
                        queue::Current::Named { .. } => 1,
                        queue::Current::Tracked(_) => 2,
                    };
                    let candidate = routes
                        .project(lease, |view| {
                            preference(view.topology.graph().current_state())
                        })
                        .ok_or_else(|| Self::missing(Replay::Current, lease))?;
                    let root = match (root, candidate) {
                        (None, _) | (_, 2) => lease,
                        (Some(root), 0) => root,
                        (Some(root), _) => {
                            let current = routes
                                .project(root, |view| {
                                    preference(view.topology.graph().current_state())
                                })
                                .ok_or_else(|| Self::missing(Replay::Current, root))?;
                            if current == 0 { lease } else { root }
                        }
                    };
                    let ticket = Self::ticket(
                        target,
                        lease,
                        Node::Open {
                            after: Some(cursor),
                            root: Some(root),
                        },
                        &Step::Open,
                    );
                    return Ok(Plan::Item(visit(Step::Open, ticket)));
                }
                Node::Confirm { after, lease, root } => {
                    let head = routes
                        .project(lease, |view| view.topology.graph().head())
                        .ok_or_else(|| Self::missing(Replay::Confirm, lease))?;
                    if !head.0 {
                        *node = Node::Qos { after, lease, root };
                        continue;
                    }
                    let step = Step::Confirm;
                    let ticket =
                        Self::ticket(target, lease, Node::Qos { after, lease, root }, &step);
                    return Ok(Plan::Item(visit(step, ticket)));
                }
                Node::Qos { after, lease, root } => {
                    let qos = routes
                        .project(lease, |view| view.topology.graph().head().1)
                        .ok_or_else(|| Self::missing(Replay::Qos, lease))?;
                    let next = Node::QueueCurrent { after, lease, root };
                    let Some(qos) = qos else {
                        *node = next;
                        continue;
                    };
                    let step = Step::Qos(qos);
                    let ticket = Self::ticket(target, lease, next, &step);
                    return Ok(Plan::Item(visit(step, ticket)));
                }
                Node::Exchange { index, root } => {
                    let output = routes
                        .project(root, |view| {
                            let graph = view.topology.graph();
                            let (next, record) = graph.exchange(index)?;
                            let step = Step::Exchange(record);
                            let ticket = Self::ticket(
                                target,
                                root,
                                Node::Exchange { index: next, root },
                                &step,
                            );
                            Some(visit(step, ticket))
                        })
                        .ok_or_else(|| Self::missing(Replay::Exchange, root))?;
                    if let Some(output) = output {
                        return Ok(Plan::Item(output));
                    }
                    *node = Node::Queue { after: None, root };
                }
                Node::Queue { after, root } => {
                    let output = routes
                        .project(root, |view| {
                            let graph = view.topology.graph();
                            let (cursor, key, record) = graph.queue(after)?;
                            let step = Step::queue(record.value(), key);
                            let ticket = Self::ticket(
                                target,
                                root,
                                Node::Queue {
                                    after: Some(cursor),
                                    root,
                                },
                                &step,
                            );
                            Some(visit(step, ticket))
                        })
                        .ok_or_else(|| Self::missing(Replay::Queue, root))?;
                    if let Some(output) = output {
                        return Ok(Plan::Item(output));
                    }
                    *node = Node::QueueBind { index: 0, root };
                }
                Node::QueueBind { index, root } => {
                    let output = routes
                        .project(root, |view| {
                            let graph = view.topology.graph();
                            let (next, spec) = graph.queue_bind(index)?;
                            let name = spec.name();
                            let entry = graph.queue_entry(spec.key(), name);
                            let queue = match &entry {
                                Some(entry) => entry.value().server_name().into(),
                                None => name,
                            };
                            let step = Step::queue_bind(spec, queue);
                            let ticket = Self::ticket(
                                target,
                                root,
                                Node::QueueBind { index: next, root },
                                &step,
                            );
                            Some(visit(step, ticket))
                        })
                        .ok_or_else(|| Self::missing(Replay::QueueBind, root))?;
                    if let Some(output) = output {
                        return Ok(Plan::Item(output));
                    }
                    *node = Node::ExchangeBind { index: 0, root };
                }
                Node::ExchangeBind { index, root } => {
                    let output = routes
                        .project(root, |view| {
                            let graph = view.topology.graph();
                            let (next, spec) = graph.binding(index)?;
                            let step = Step::ExchangeBind(spec);
                            let ticket = Self::ticket(
                                target,
                                root,
                                Node::ExchangeBind { index: next, root },
                                &step,
                            );
                            Some(visit(step, ticket))
                        })
                        .ok_or_else(|| Self::missing(Replay::ExchangeBind, root))?;
                    if let Some(output) = output {
                        return Ok(Plan::Item(output));
                    }
                    *node = Node::Current { after: None, root };
                }
                Node::Current { after, root } => {
                    let Some((cursor, lease)) = routes.next_tracked(after) else {
                        *node = Node::Done;
                        continue;
                    };
                    let reset = lease == root
                        && routes
                            .project(lease, |view| {
                                let graph = view.topology.graph();
                                matches!(graph.current_state(), queue::Current::None)
                                    && graph.has_queues()
                            })
                            .ok_or_else(|| Self::missing(Replay::Current, lease))?;
                    if reset {
                        let step = Step::Close;
                        let ticket = Self::ticket(
                            target,
                            lease,
                            Node::Reopen {
                                after: cursor,
                                root,
                            },
                            &step,
                        );
                        return Ok(Plan::Item(visit(step, ticket)));
                    }
                    *node = Node::Confirm {
                        after: cursor,
                        lease,
                        root,
                    };
                }
                Node::Reopen { after, root } => {
                    let step = Step::Open;
                    let ticket = Self::ticket(
                        target,
                        root,
                        Node::Confirm {
                            after,
                            lease: root,
                            root,
                        },
                        &step,
                    );
                    return Ok(Plan::Item(visit(step, ticket)));
                }
                Node::QueueCurrent { after, lease, root } => {
                    let next = Node::Consumer {
                        after,
                        lease,
                        item: None,
                        root,
                    };
                    if root == lease {
                        *node = next;
                        continue;
                    }
                    let output = routes
                        .project(lease, |view| {
                            let graph = view.topology.graph();
                            let (key, record) = graph.current()?;
                            let step = Step::current(record.value(), key);
                            let ticket = Self::ticket(target, lease, next, &step);
                            Some(visit(step, ticket))
                        })
                        .ok_or_else(|| Self::missing(Replay::Current, lease))?;
                    if let Some(output) = output {
                        return Ok(Plan::Item(output));
                    }
                    *node = next;
                }
                Node::Consumer {
                    after,
                    lease,
                    item,
                    root,
                } => {
                    let output = routes
                        .project(lease, |view| {
                            if view.channel.method_state().methods.next::<true>().0 {
                                return ops::ControlFlow::Break(());
                            }
                            let mut item = item;
                            loop {
                                let Some(replay) =
                                    view.channel.consumers().read().next_replay(item)
                                else {
                                    return ops::ControlFlow::Continue(None);
                                };
                                let cursor = replay.cursor();
                                let spec = replay.spec();
                                let graph = view.topology.graph();
                                let key = spec.key();
                                let name = spec.name();
                                let entry = graph.queue_entry(key, name);
                                let queue = match &entry {
                                    Some(entry) => entry.value().server_name().into(),
                                    None => match key {
                                        None => name,
                                        Some(key) => {
                                            view.channel.consumers().forget_key(key);
                                            item = Some(cursor);
                                            continue;
                                        }
                                    },
                                };
                                let step = Step::consume(spec, queue, replay.token());
                                let next = Node::Consumer {
                                    after,
                                    lease,
                                    item: Some(cursor),
                                    root,
                                };
                                let ticket = Self::ticket(target, lease, next, &step);
                                return ops::ControlFlow::Continue(Some(visit(step, ticket)));
                            }
                        })
                        .ok_or_else(|| Self::missing(Replay::Consumer, lease))?;
                    let output = match output {
                        ops::ControlFlow::Break(()) => {
                            self.status = Status::Waiting {
                                target,
                                lease,
                                node: *node,
                            };
                            return Ok(Plan::Idle);
                        }
                        ops::ControlFlow::Continue(output) => output,
                    };
                    if let Some(output) = output {
                        return Ok(Plan::Item(output));
                    }
                    *node = Node::Current {
                        after: Some(after),
                        root,
                    };
                }
                Node::Done => {
                    self.status = Status::Idle;
                    return Ok(Plan::Ready(target));
                }
            }
        }
    }

    fn ticket(
        target: epoch::Id<'d, ID>,
        lease: channels::Lease<'d, ID>,
        next: Node<'c, 'd, ID>,
        step: &Step<'_, '_, 'd, consumer::Key, ID>,
    ) -> Ticket<'c, 'd, ID> {
        Ticket {
            target,
            lease,
            next,
            intent: step.intent(),
        }
    }
}

const _: () = assert!(mem::size_of::<Recovery<'_, '_, 0>>() == 8 * mem::size_of::<usize>());
const _: () = assert!(mem::align_of::<Recovery<'static, 'static, 0>>() == mem::align_of::<usize>());
