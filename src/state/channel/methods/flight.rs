use std::{mem, num};

use o3::mem::rc;

use crate::failure::wire::{self, turn};
use crate::protocol::topology;
use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::schema::queues;
use crate::protocol::wire::text::{resident, short};
use crate::state::channel::{consumer, methods};

struct Head<'c, 'b, 'd, const ID: u8> {
    method: methods::MethodKey,
    response: Prepared<'c, 'b, 'd, ID>,
}

pub(crate) enum Prepared<'c, 'b, 'd, const ID: u8> {
    None,
    Work(topology::Work<'c, 'b, 'd, ID>),
    Identity {
        writer: resident::Writer<'b>,
        work: Option<topology::Work<'c, 'b, 'd, ID>>,
    },
    Refresh(topology::Refresh<'c, 'b, 'd, ID>),
    ConsumerNew {
        key: consumer::Key,
        identity: Option<resident::Writer<'b>>,
    },
    ConsumerReplay(consumer::Key),
}

pub(crate) enum Consumer<'a, 'c, 'b, 'd, const ID: u8> {
    Named(Named<'a, 'c, 'b, 'd, ID>),
    Fresh(Fresh<'a, 'c, 'b, 'd, ID>),
    Replay(Replay<'a, 'c, 'b, 'd, ID>),
}

pub(crate) struct Named<'a, 'c, 'b, 'd, const ID: u8> {
    owner: &'a mut Flight<'c, 'b, 'd, ID>,
    method: methods::MethodKey,
    key: consumer::Key,
}

pub(crate) struct Fresh<'a, 'c, 'b, 'd, const ID: u8> {
    owner: &'a mut Flight<'c, 'b, 'd, ID>,
    method: methods::MethodKey,
    key: consumer::Key,
    identity: resident::Writer<'b>,
}

pub(crate) struct Replay<'a, 'c, 'b, 'd, const ID: u8> {
    owner: &'a mut Flight<'c, 'b, 'd, ID>,
    method: methods::MethodKey,
    key: consumer::Key,
}

pub(crate) enum Fault<'b, 'd> {
    Mismatch(resident::Short<'b, 'd>),
    Protocol(wire::Fault),
    Retain(rc::Error),
}

impl<const ID: u8> Named<'_, '_, '_, '_, ID> {
    pub(crate) const fn key(&self) -> consumer::Key {
        self.key
    }

    pub(crate) fn commit(self) -> consumer::Key {
        self.key
    }

    pub(crate) fn restore(self) {
        self.owner.head = Some(Head {
            method: self.method,
            response: Prepared::ConsumerNew {
                key: self.key,
                identity: None,
            },
        });
    }
}

impl<'c, 'b, 'd, const ID: u8> Fresh<'_, 'c, 'b, 'd, ID> {
    pub(crate) const fn key(&self) -> consumer::Key {
        self.key
    }

    pub(crate) fn commit(self) -> (consumer::Key, resident::Writer<'b>) {
        (self.key, self.identity)
    }

    pub(crate) fn restore(self) {
        self.owner.head = Some(Head {
            method: self.method,
            response: Prepared::ConsumerNew {
                key: self.key,
                identity: Some(self.identity),
            },
        });
    }
}

impl<const ID: u8> Replay<'_, '_, '_, '_, ID> {
    pub(crate) const fn key(&self) -> consumer::Key {
        self.key
    }

    pub(crate) fn commit(self) -> consumer::Key {
        self.key
    }

    pub(crate) fn restore(self) {
        self.owner.head = Some(Head {
            method: self.method,
            response: Prepared::ConsumerReplay(self.key),
        });
    }
}

pub(crate) struct Flight<'c, 'b, 'd, const ID: u8> {
    head: Option<Head<'c, 'b, 'd, ID>>,
}

impl<const ID: u8> Prepared<'_, '_, '_, ID> {
    fn phase(&self) -> turn::Flight {
        match self {
            Self::None => turn::Flight::Empty,
            Self::Work(_) => turn::Flight::Work,
            Self::Identity { .. } => turn::Flight::Identity,
            Self::Refresh(refresh) => refresh.phase(),
            Self::ConsumerNew { .. } => turn::Flight::ConsumerNew,
            Self::ConsumerReplay(_) => turn::Flight::ConsumerReplay,
        }
    }
}

impl<'c, 'b, 'd, const ID: u8> Flight<'c, 'b, 'd, ID> {
    pub(super) const fn new() -> Self {
        Self { head: None }
    }

    pub(super) fn replays(&self, key: consumer::Key) -> bool {
        self.head.as_ref().is_some_and(
            |head| matches!(head.response, Prepared::ConsumerReplay(actual) if actual == key),
        )
    }

    pub(super) fn check(&self, key: methods::MethodKey) -> Result<(), wire::Fault> {
        let Some(head) = self.head.as_ref() else {
            return Ok(());
        };
        if head.method == key {
            Ok(())
        } else {
            Err(wire::Fault::FlightMethod {
                expected: num::NonZero::from(key),
                actual: num::NonZero::from(head.method),
            })
        }
    }

    pub(super) fn vacant(&self, key: methods::MethodKey) -> Result<(), wire::Fault> {
        if let Some(head) = self.head.as_ref() {
            return if head.method == key {
                Err(wire::Fault::FlightState {
                    method: num::NonZero::from(key),
                    expected: turn::Use::Vacant,
                    actual: head.response.phase(),
                })
            } else {
                Err(wire::Fault::FlightMethod {
                    expected: num::NonZero::from(key),
                    actual: num::NonZero::from(head.method),
                })
            };
        }
        Ok(())
    }

    pub(super) fn put(&mut self, key: methods::MethodKey, response: Prepared<'c, 'b, 'd, ID>) {
        if matches!(&response, Prepared::None) {
            return;
        }
        self.head = Some(Head {
            method: key,
            response,
        });
    }

    pub(crate) fn retain_queue(
        &mut self,
        key: methods::MethodKey,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        value: short::Name<'_>,
    ) -> Result<resident::Short<'b, 'd>, Fault<'b, 'd>> {
        let (name, release) = {
            let head = self
                .head
                .as_mut()
                .ok_or(Fault::Protocol(wire::Fault::FlightState {
                    method: num::NonZero::from(key),
                    expected: turn::Use::Queue,
                    actual: turn::Flight::Empty,
                }))?;
            let phase = head.response.phase();
            if let Prepared::Work(work) = &head.response {
                let name = match work {
                    topology::Work::QueueDeclare(queue, _) => queue
                        .try_field(queues::Declaration::QUEUE)
                        .map_err(Fault::Retain)?,
                    topology::Work::QueueRedeclare { key: queue, .. } => topo
                        .queue_name(*queue)
                        .map_err(Fault::Retain)?
                        .ok_or(Fault::Protocol(wire::Fault::TopologyQueue {
                            queue: queue.identity(),
                        }))?,
                    _ => {
                        return Err(Fault::Protocol(wire::Fault::FlightState {
                            method: num::NonZero::from(key),
                            expected: turn::Use::Queue,
                            actual: phase,
                        }));
                    }
                };
                if name.as_ref() != value.value() {
                    return Err(Fault::Mismatch(name));
                }
                return Ok(name);
            }
            match mem::replace(&mut head.response, Prepared::None) {
                Prepared::Identity { writer, work } => {
                    let hash = Fingerprint::of(value.value().as_bytes()).get();
                    let (name, work) = match work {
                        Some(topology::Work::QueueDeclare(record, plan)) => {
                            let (stored, name) = writer.commit_pair(value.into(), hash);
                            let key = topo.declare(record, plan, Some(stored));
                            (
                                name,
                                Some(topology::Work::QueueRedeclare {
                                    key,
                                    update_current: false,
                                }),
                            )
                        }
                        work => {
                            let name = writer.commit(value.into(), hash);
                            if work.is_none() {
                                topo.inspect(value, hash);
                            }
                            (name, work)
                        }
                    };
                    let release = work.is_none();
                    head.response = work.map_or_else(|| Prepared::None, Prepared::Work);
                    (name.into(), release)
                }
                Prepared::Refresh(refresh) => {
                    let (key, name) = refresh.commit(topo, value);
                    head.response = Prepared::Work(topology::Work::QueueRedeclare {
                        key,
                        update_current: false,
                    });
                    (name, false)
                }
                other => {
                    head.response = other;
                    return Err(Fault::Protocol(wire::Fault::FlightState {
                        method: num::NonZero::from(key),
                        expected: turn::Use::Queue,
                        actual: phase,
                    }));
                }
            }
        };
        if release {
            self.head = None;
        }
        Ok(name)
    }

    pub(super) fn consumer(
        &mut self,
        key: methods::MethodKey,
    ) -> Result<Consumer<'_, 'c, 'b, 'd, ID>, wire::Fault> {
        let head = self.head.take().ok_or(wire::Fault::FlightState {
            method: num::NonZero::from(key),
            expected: turn::Use::Consumer,
            actual: turn::Flight::Empty,
        })?;
        match head {
            Head {
                method,
                response:
                    Prepared::ConsumerNew {
                        key,
                        identity: None,
                    },
            } => Ok(Consumer::Named(Named {
                owner: self,
                method,
                key,
            })),
            Head {
                method,
                response:
                    Prepared::ConsumerNew {
                        key,
                        identity: Some(identity),
                    },
            } => Ok(Consumer::Fresh(Fresh {
                owner: self,
                method,
                key,
                identity,
            })),
            Head {
                method,
                response: Prepared::ConsumerReplay(key),
            } => Ok(Consumer::Replay(Replay {
                owner: self,
                method,
                key,
            })),
            head => {
                let actual = head.response.phase();
                self.head = Some(head);
                Err(wire::Fault::FlightState {
                    method: num::NonZero::from(key),
                    expected: turn::Use::Consumer,
                    actual,
                })
            }
        }
    }

    pub(super) fn complete(
        &mut self,
        key: methods::MethodKey,
    ) -> Result<Option<topology::Work<'c, 'b, 'd, ID>>, wire::Fault> {
        let Some(head) = self.head.take() else {
            return Ok(None);
        };
        match head.response {
            Prepared::None => Ok(None),
            Prepared::Work(work) => Ok(Some(work)),
            response => {
                let actual = response.phase();
                self.head = Some(Head {
                    method: head.method,
                    response,
                });
                Err(wire::Fault::FlightState {
                    method: num::NonZero::from(key),
                    expected: turn::Use::Complete,
                    actual,
                })
            }
        }
    }

    pub(super) fn cancel(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Option<consumer::Rollback> {
        let head = self.head.take()?;
        match head.response {
            Prepared::ConsumerNew { key, .. } => Some(consumer::Rollback::New(key)),
            Prepared::ConsumerReplay(key) => Some(consumer::Rollback::Replay(key)),
            Prepared::Work(work) => {
                work.undo(topo);
                None
            }
            Prepared::Identity { work, .. } => {
                if let Some(work) = work {
                    work.undo(topo);
                }
                None
            }
            Prepared::Refresh(refresh) => {
                refresh.undo(topo);
                None
            }
            Prepared::None => None,
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.head.is_none()
    }
}

const _: () = {
    assert!(mem::size_of::<Head<'_, '_, '_, 0>>() <= 8 * mem::size_of::<u64>());
    assert!(mem::size_of::<Flight<'_, '_, '_, 0>>() <= 8 * mem::size_of::<u64>());
};
