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
    member: methods::Member<'a, 'c, 'b, 'd, methods::Plane>,
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
        let member = cursor.release();
        Some(Self {
            key,
            wakes,
            member,
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
        let member = cursor.release();
        Ok(Self {
            key,
            wakes,
            member,
            flight,
        })
    }

    fn begin(
        owner: &'a mut methods::Methods<'c, 'b, 'd, ID>,
        expect: method::Kind,
    ) -> Result<Self, wire::Fault> {
        let front = Self::take(owner)?;
        if front.member.get().expect == expect {
            return Ok(front);
        }
        let expected = front.member.get().expect;
        Err(wire::Fault::Response {
            expected,
            actual: expect,
        })
    }

    fn discard(self) {
        self.member.remove().discard();
    }

    fn reply(
        self,
        outcome: Result<protocol::Response<'b, 'd, consumer::Key>, terminal::Outcome<'c, 'b, 'd>>,
    ) {
        self.member.remove().reply(outcome, self.wakes);
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
        ok: basic::CancelOk<'d>,
    ) -> Result<methods::Next<'b, 'd>, failure::Error<resident::Short<'b, 'd>>> {
        let front = Front::begin(self.methods, method::Kind::CancelOk)
            .map_err(failure::Error::<resident::Short<'b, 'd>>::Protocol)?;
        let expected = match &front.member.get().outbound {
            entry::Outbound::Consumer(key) => self.consumers.read().retiree(*key)?,
            entry::Outbound::Orphan(tag) => tag.try_clone().map_err(|error| failure::Error::<
                resident::Short<'b, 'd>,
            >::Resident {
                target: wire::Allocation::BrokerConsumer,
                error: wire::Resident::Retain(error),
            })?,
            entry::Outbound::None | entry::Outbound::Close => {
                let actual = front.member.get().control();
                let method = num::NonZero::from(front.key);
                return Err(failure::Error::Protocol(wire::Fault::MethodControl {
                    method,
                    expected: turn::Control::Cancel,
                    actual,
                }));
            }
        };
        if expected.as_ref() != ok.consumer_tag.as_str() {
            return Err(failure::Error::ConsumerTag {
                expected,
                actual: ok.consumer_tag.into(),
            });
        }
        let origin = front.member.get().origin;
        let retire = front.member.get().retiree();
        // Controls are detached or replay-owned and cannot retain Flight work.
        front.discard();
        let freed = match retire {
            Some(key) => self.consumers.detach(key, self.deliveries),
            None => channel::Freed::Available,
        };
        Ok(self.next(origin, freed))
    }

    pub(crate) fn retain_queue(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        name: short::Name<'_>,
    ) -> Result<resident::Short<'b, 'd>, flight::Fault<'b, 'd>> {
        let front = Front::begin(self.methods, method::Kind::QueueDeclare)
            .map_err(flight::Fault::Protocol)?;
        front.flight.retain_queue(front.key, topo, name)
    }

    pub(crate) fn complete(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        reply: protocol::Response<'b, 'd, consumer::Key>,
    ) -> Result<(methods::Next<'b, 'd>, Option<queue::Delete<'d, ID>>), failure::Error> {
        let expect = reply.kind();
        let front = Front::begin(self.methods, expect).map_err(failure::Error::Protocol)?;
        let origin = front.member.get().origin;
        let work = front
            .flight
            .complete(front.key)
            .map_err(failure::Error::Protocol)?;
        let deletion = if let Some(work) = work {
            match work.commit(topo) {
                Ok(deletion) => deletion,
                Err(failure) => {
                    let (work, cause) = failure.split();
                    front.flight.put(front.key, flight::Prepared::Work(work));
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
        Ok((self.next(origin, channel::Freed::Available), deletion))
    }

    pub(crate) fn consume(
        &mut self,
        name: short::Short<'_>,
    ) -> Result<methods::Next<'b, 'd>, access::Fault<'b, 'd>> {
        let front =
            Front::begin(self.methods, method::Kind::Consume).map_err(access::Fault::Protocol)?;
        let origin = front.member.get().origin;
        let consumer = front
            .flight
            .consumer(front.key)
            .map_err(access::Fault::Protocol)?;
        let (reply, cancel) = match consumer {
            flight::Consumer::Named(consumer) => {
                let key = consumer.key();
                let pending = match self.consumers.write().named(key, name) {
                    Ok(pending) => pending,
                    Err(error) => {
                        consumer.restore();
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

    #[inline(never)]
    pub(crate) fn fail(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        cause: &terminal::Lease<'c, 'b, 'd>,
    ) {
        let rollback = self.methods.flight.cancel(topo);
        self.consumers.rollback(rollback);
        while let Some(front) = Front::fail(self.methods) {
            let retire = front.member.get().retiree();
            if front.member.get().origin == entry::Origin::User {
                let outcome = terminal::Outcome::retain(cause);
                front.reply(Err(outcome));
            } else {
                front.discard();
            }
            if let Some(key) = retire {
                let _ = self.consumers.detach(key, self.deliveries);
            }
        }
        self.consumers.disconnect();
    }

    fn next(&mut self, origin: entry::Origin, freed: channel::Freed) -> methods::Next<'b, 'd> {
        if origin == entry::Origin::Replay {
            let (pending, next) = self.methods.next::<true>();
            return match next {
                Some(next) => methods::Next::Resume { next, freed },
                None if pending => methods::Next::Idle { freed },
                None => methods::Next::Replay { freed },
            };
        }
        if !self.publish_active
            && let Some(next) = self.methods.next::<false>().1
        {
            return methods::Next::Resume { next, freed };
        }
        methods::Next::Idle { freed }
    }
}
