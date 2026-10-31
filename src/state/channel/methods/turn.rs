use std::num;

use dope::core::driver::schedule::ready::wake::store;

use crate::failure::wire::turn;
use crate::failure::{self, terminal, wire};
use crate::protocol::method::basic;
use crate::protocol::topology::records::queue;
use crate::protocol::wire::text::{resident, short};
use crate::protocol::{self, method, topology};
use crate::state::channel::consumer::{access, delivery};
use crate::state::channel::methods::{entry, flight};
use crate::state::channel::{self, consumer, methods};

struct Front<'a, 'c, 'b, 'd, const ID: u8> {
    key: methods::MethodKey,
    wakes: store::Store<'d>,
    detached: methods::Detached<'a, 'c, 'b, 'd, methods::Plane>,
    flight: &'a mut flight::Flight<'c, 'b, 'd, ID>,
}

pub(crate) struct Turn<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    methods: &'a mut methods::Methods<'c, 'b, 'd, ID>,
    consumers: &'a mut consumer::Set<'b, 'd, ID>,
    deliveries: &'r delivery::Store<'c, 'b, 'd>,
    publish_active: bool,
}

impl<'c, 'b, 'a, 'd, const ID: u8> Front<'a, 'c, 'b, 'd, ID> {
    fn fail(owner: &'a mut methods::Methods<'c, 'b, 'd, ID>) -> Option<Self> {
        let (cursor, flight, wakes) = methods::Slot::front(owner)?;
        let key = cursor.key();
        let detached = cursor.remove().release();
        Some(Self {
            key,
            wakes,
            detached,
            flight,
        })
    }

    fn take(owner: &'a mut methods::Methods<'c, 'b, 'd, ID>) -> Result<Self, wire::Fault> {
        let (cursor, flight, wakes) =
            methods::Slot::front(owner).ok_or(wire::Fault::MethodState {
                method: None,
                expected: turn::Method::Pending,
                actual: turn::Method::Missing,
            })?;
        let key = cursor.key();
        flight.check(key)?;
        let detached = cursor.remove().release();
        Ok(Self {
            key,
            wakes,
            detached,
            flight,
        })
    }

    fn begin(
        owner: &'a mut methods::Methods<'c, 'b, 'd, ID>,
        expect: method::Kind,
    ) -> Result<Self, wire::Fault> {
        let front = Self::take(owner)?;
        if front.detached.pending().expect == expect {
            return Ok(front);
        }
        let expected = front.detached.pending().expect;
        front.restore();
        Err(wire::Fault::Response {
            expected,
            actual: expect,
        })
    }

    fn restore(self) {
        self.detached.restore();
    }

    fn discard(self) {
        self.detached.discard();
    }

    fn reply(
        self,
        outcome: Result<protocol::Response<'b, 'd, consumer::Key>, terminal::Outcome<'c, 'b, 'd>>,
    ) {
        self.detached.reply(outcome, self.wakes);
    }
}

impl<'r, 'c, 'b, 'a, 'd, const ID: u8> Turn<'a, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::state::channel) fn new(
        ops: &'a mut methods::Methods<'c, 'b, 'd, ID>,
        users: &'a mut consumer::Set<'b, 'd, ID>,
        msgs: &'r delivery::Store<'c, 'b, 'd>,
        active: bool,
    ) -> Self {
        Self {
            methods: ops,
            consumers: users,
            deliveries: msgs,
            publish_active: active,
        }
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Turn<'_, 'r, 'c, 'b, 'd, ID> {
    pub(crate) fn cancel(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        ok: basic::CancelOk<'d>,
    ) -> Result<methods::Next<'b, 'd>, failure::Error<resident::Short<'b, 'd>>> {
        let front = Front::begin(self.methods, method::Kind::CancelOk)
            .map_err(failure::Error::<resident::Short<'b, 'd>>::Protocol)?;
        let expected = match &front.detached.pending().outbound {
            entry::Outbound::Consumer(key) => match self.consumers.read().retiree(*key) {
                Ok(tag) => tag,
                Err(error) => {
                    front.restore();
                    return Err(error);
                }
            },
            entry::Outbound::Orphan(tag) => match tag.try_clone() {
                Ok(tag) => tag,
                Err(error) => {
                    front.restore();
                    return Err(failure::Error::Resident {
                        target: wire::Allocation::BrokerConsumer,
                        error: wire::Resident::Retain(error),
                    });
                }
            },
            entry::Outbound::None | entry::Outbound::Close => {
                let actual = front.detached.pending().control();
                let method = num::NonZero::from(front.key);
                front.restore();
                return Err(failure::Error::Protocol(wire::Fault::MethodControl {
                    method,
                    expected: turn::Control::Cancel,
                    actual,
                }));
            }
        };
        if expected.as_ref() != ok.consumer_tag.as_str() {
            front.restore();
            return Err(failure::Error::ConsumerTag {
                expected,
                actual: ok.consumer_tag.into(),
            });
        }
        front.restore();
        self.complete(topo, method::Kind::CancelOk, ok.into())
            .map(|(next, _)| next)
            .map_err(failure::Error::from)
    }

    pub(crate) fn retain_queue(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        name: short::Name<'_>,
    ) -> Result<resident::Short<'b, 'd>, flight::Fault<'b, 'd>> {
        let front = Front::begin(self.methods, method::Kind::QueueDeclare)
            .map_err(flight::Fault::Protocol)?;
        let retained = front.flight.retain_queue(front.key, topo, name);
        front.restore();
        retained
    }

    pub(crate) fn complete(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        expect: method::Kind,
        reply: protocol::Response<'b, 'd, consumer::Key>,
    ) -> Result<(methods::Next<'b, 'd>, Option<queue::Delete<'d, ID>>), failure::Error> {
        let actual = reply.kind();
        if actual != expect {
            return Err(failure::Error::Protocol(wire::Fault::Response {
                expected: expect,
                actual,
            }));
        }
        let front = Front::begin(self.methods, expect).map_err(failure::Error::Protocol)?;
        let origin = front.detached.pending().origin;
        let retire = front.detached.pending().retiree();
        let work = match front.flight.complete(front.key) {
            Ok(work) => work,
            Err(error) => {
                front.restore();
                return Err(failure::Error::Protocol(error));
            }
        };
        let deletion = if let Some(work) = work {
            let ready = match work.prepare(&reply) {
                Ok(ready) => ready,
                Err(failure) => {
                    let (work, cause) = failure.split();
                    front.flight.put(front.key, flight::Prepared::Work(work));
                    front.restore();
                    return Err(cause);
                }
            };
            match ready.commit(topo) {
                Ok(deletion) => deletion,
                Err(failure) => {
                    let (work, cause) = failure.split();
                    front.flight.put(front.key, flight::Prepared::Work(work));
                    front.restore();
                    return Err(cause);
                }
            }
        } else {
            None
        };
        if expect == method::Kind::Open {
            topo.track();
        }
        let detach = origin == entry::Origin::Detached && expect == method::Kind::Open;
        front.reply(Ok(reply));
        if detach {
            return Ok((
                methods::Next::Close {
                    freed: channel::Freed::Available,
                },
                deletion,
            ));
        }
        let freed = match retire {
            Some(key) => match self.consumers.detach(key, self.deliveries) {
                consumer::Removal::Removed(freed) => freed,
                consumer::Removal::Missing => channel::Freed::Available,
            },
            None => channel::Freed::Available,
        };
        Ok((self.next(origin, freed), deletion))
    }

    pub(crate) fn consume(
        &mut self,
        name: short::Short<'_>,
    ) -> Result<methods::Next<'b, 'd>, access::Fault<'b, 'd>> {
        let front =
            Front::begin(self.methods, method::Kind::Consume).map_err(access::Fault::Protocol)?;
        let origin = front.detached.pending().origin;
        let consumer = match front.flight.consumer(front.key) {
            Ok(consumer) => consumer,
            Err(error) => {
                front.restore();
                return Err(access::Fault::Protocol(error));
            }
        };
        let (reply, cancel) = match consumer {
            flight::Consumer::Named(consumer) => {
                let key = consumer.key();
                let pending = match self.consumers.write().named(key, name) {
                    Ok(pending) => pending,
                    Err(error) => {
                        consumer.restore();
                        front.restore();
                        return Err(error);
                    }
                };
                if origin == entry::Origin::Detached {
                    let key = consumer.commit();
                    pending.retire();
                    (None, Some(consumer::Cancel::Tracked(key)))
                } else {
                    let tag = match pending.try_commit() {
                        Ok(tag) => tag,
                        Err(error) => {
                            consumer.restore();
                            front.restore();
                            return Err(access::Fault::Retain(error));
                        }
                    };
                    let key = consumer.commit();
                    let method = basic::ConsumeOk { consumer_tag: tag };
                    let done = protocol::Registration::Registered(method, key);
                    (Some(done.into()), None)
                }
            }
            flight::Consumer::Fresh(consumer) => {
                let key = consumer.key();
                let pending = match self.consumers.write().fresh(key, name) {
                    Ok(pending) => pending,
                    Err(error) => {
                        consumer.restore();
                        front.restore();
                        return Err(error);
                    }
                };
                let (key, writer) = consumer.commit();
                if origin == entry::Origin::Detached {
                    pending.retire(writer);
                    (None, Some(consumer::Cancel::Tracked(key)))
                } else {
                    let method = basic::ConsumeOk {
                        consumer_tag: pending.commit(writer),
                    };
                    let done = protocol::Registration::Registered(method, key);
                    (Some(done.into()), None)
                }
            }
            flight::Consumer::Replay(consumer) => {
                use consumer::replay::Completion;

                let key = consumer.key();
                let replay = match self.consumers.write().finish(key, name) {
                    Ok(replay) => replay,
                    Err(error) => {
                        consumer.restore();
                        front.restore();
                        return Err(error);
                    }
                };
                consumer.commit();
                let cancel = match replay {
                    Completion::Canceled(tag) => Some(consumer::Cancel::Orphan(tag)),
                    Completion::Active | Completion::Retiring => None,
                };
                let reply = (origin == entry::Origin::User)
                    .then_some(protocol::Response::from(protocol::Registration::Replayed));
                (reply, cancel)
            }
        };
        if let Some(reply) = reply {
            front.reply(Ok(reply));
        } else {
            front.discard();
        }
        match cancel {
            Some(consumer::Cancel::Orphan(tag)) if origin == entry::Origin::Replay => {
                Ok(methods::Next::ReplayCancel {
                    cancel: tag,
                    freed: channel::Freed::Available,
                })
            }
            Some(cancel) => Ok(methods::Next::Cancel {
                cancel,
                freed: channel::Freed::Available,
            }),
            None => Ok(self.next(origin, channel::Freed::Available)),
        }
    }

    pub(crate) fn fail(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        cause: &terminal::Lease<'c, 'b, 'd>,
    ) -> channel::Freed {
        let mut freed = channel::Freed::Available;
        let rollback = self.methods.flight.cancel(topo);
        self.consumers.rollback(rollback);
        while let Some(front) = Front::fail(self.methods) {
            let retire = front.detached.pending().retiree();
            if front.detached.pending().origin == entry::Origin::User {
                let outcome = terminal::Outcome::retain(cause);
                front.reply(Err(outcome));
            } else {
                front.discard();
            }
            if let Some(key) = retire
                && let consumer::Removal::Removed(route) =
                    self.consumers.detach(key, self.deliveries)
            {
                freed = freed.merge(route);
            }
        }
        self.consumers.disconnect();
        freed
    }

    fn next(&mut self, origin: entry::Origin, freed: channel::Freed) -> methods::Next<'b, 'd> {
        if origin == entry::Origin::Replay {
            return methods::Next::Replay { freed };
        }
        if !self.publish_active
            && let Some(next) = self.methods.next()
        {
            return methods::Next::Resume { next, freed };
        }
        methods::Next::Idle { freed }
    }
}
