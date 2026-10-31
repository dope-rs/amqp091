use std::num;

use consumer::delivery;
use dope::core::driver::schedule::ready::wake;
use o3::buffer::resident::Shared;
use publish::{confirms, streams};

use crate::failure::{self, terminal, wire};
use crate::message::field::view;
use crate::message::property;
use crate::protocol::topology::need;
use crate::protocol::wire::text::{resident, short};
use crate::protocol::{self, method, replays, topology};
use crate::service::content::wait::resource;
use crate::service::content::wait::wake::Wake;
use crate::service::content::{payload, pending};
use crate::state::channel::cancel::lanes;
use crate::state::channel::methods::{self, flight, send};
use crate::state::channel::{self, consumer, inbound, publish, returns};
use crate::state::work;

pub(crate) struct Method<'a, 'c, 'b, 'd, const ID: u8> {
    pub(crate) phase: &'a mut channel::Phase,
    pub(crate) methods: &'a mut methods::Methods<'c, 'b, 'd, ID>,
    pub(crate) consumers: &'a mut consumer::Set<'b, 'd, ID>,
    pub(crate) publishing: &'a mut streams::Flight,
}

#[must_use = "the reserved method must be committed with its frame"]
pub(crate) struct Permit<'a, 'c, 'b, 'd, const ID: u8> {
    method: methods::Permit<'a, 'c, 'b, 'd, ID>,
    consumers: &'a mut consumer::Set<'b, 'd, ID>,
}

pub(crate) struct Publish<'a, 'c, 'b, 'd, const ID: u8> {
    pub(crate) control: &'a mut channel::Control,
    pub(crate) methods: &'a mut methods::Methods<'c, 'b, 'd, ID>,
    pub(super) confirm: &'a mut confirms::Ledger<'c, 'b, 'd>,
    pub(crate) publishing: &'a mut streams::Flight,
}

pub(crate) struct Confirm<'a, 'c, 'b, 'd> {
    pub(crate) control: &'a mut channel::Control,
    pub(super) confirm: &'a mut confirms::Ledger<'c, 'b, 'd>,
}

pub(crate) struct Content<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    pub(super) phase: channel::Phase,
    pub(super) inbound: &'a mut inbound::Inbound<'r, 'c, 'b, 'd>,
    pub(super) returns: &'a mut returns::Queue,
    pub(super) consumers: &'a mut consumer::Set<'b, 'd, ID>,
    pub(super) stores: channel::Stores<'r, 'c, 'b, 'd>,
    pub(super) maximum: usize,
}

pub(crate) struct Protocol {
    pub(crate) phase: channel::Phase,
    pub(crate) expected: Option<method::Kind>,
    pub(crate) inbound_idle: bool,
}

impl<'c, 'b, 'd, const ID: u8> Method<'_, 'c, 'b, 'd, ID> {
    pub(crate) fn retire<'r>(
        &mut self,
        key: consumer::Key,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> consumer::Retire {
        self.consumers.retire(key, stores.deliveries)
    }

    pub(crate) fn detach<'r>(
        &mut self,
        key: consumer::Key,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> consumer::Removal {
        self.consumers.detach(key, stores.deliveries)
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Content<'_, 'r, 'c, 'b, 'd, ID> {
    pub(crate) fn delivery(
        &mut self,
        tag: &str,
    ) -> Result<delivery::Route<'r, 'c, 'b, 'd>, resource::Slot> {
        self.consumers
            .reserve(tag, self.stores.deliveries)
            .ok_or(resource::Slot::Route)
    }

    pub(crate) fn reserve(&self) -> Result<returns::Vacant<'r, 'c, 'b, 'd>, resource::Slot> {
        self.stores.returns.vacant().ok_or(resource::Slot::Route)
    }

    pub(crate) fn cancel(&mut self, tag: &str) -> channel::Freed {
        self.consumers.cancel(tag, self.stores.deliveries)
    }

    pub(crate) fn begin(
        &mut self,
        entry: pending::Pending<'r, 'c, 'b, 'd>,
    ) -> Result<(), failure::Error> {
        self.inbound.begin(self.phase, entry)
    }

    pub(crate) fn header<E: From<failure::Error>>(
        &mut self,
        size: u64,
        wake: Wake<'b, 'd>,
        bind: impl FnOnce(
            usize,
        ) -> Result<
            (
                property::Properties<Shared<'d>, view::TableSpan>,
                bool,
                payload::Payload<'b, 'd>,
            ),
            E,
        >,
    ) -> Result<(), E> {
        let done = self.inbound.header(size, self.maximum, wake, bind)?;
        self.route(done);
        Ok(())
    }

    pub(crate) fn body<E: From<failure::Error>>(
        &mut self,
        length: usize,
        wake: Wake<'b, 'd>,
        store: impl FnOnce(&mut payload::Payload<'b, 'd>) -> Result<(), E>,
    ) -> Result<(), E> {
        let done = self.inbound.body(length, wake, store)?;
        self.route(done);
        Ok(())
    }

    pub(crate) fn next_return<'wait>(
        &mut self,
        wait: &mut wake::table::Registration<'wait, 'd>,
        wake: wake::Waker<'d>,
    ) -> Result<returns::Next<'c, 'b, 'd>, wake::table::Error>
    where
        'r: 'wait,
    {
        self.returns
            .next(self.phase, wait, wake, self.stores.returns)
    }

    fn route(&mut self, done: Option<pending::Complete<'r, 'c, 'b, 'd>>) {
        match done {
            Some(pending::Complete::Delivery(Some(claim))) => {
                self.consumers.route(claim, self.stores.deliveries);
            }
            Some(pending::Complete::Delivery(None)) | None => {}
            Some(pending::Complete::Returned(claim)) => {
                claim.push(self.returns, self.stores.returns)
            }
        }
    }
}

impl<'c, 'b, 'a, 'd, const ID: u8> Method<'a, 'c, 'b, 'd, ID> {
    pub(crate) fn reserve<Text>(
        self,
        key: methods::MethodKey,
    ) -> Result<Option<Permit<'a, 'c, 'b, 'd, ID>>, failure::Error<Text>> {
        let active = self.publishing.is_active();
        Ok(self.methods.reserve(key, active)?.map(|method| Permit {
            method,
            consumers: self.consumers,
        }))
    }

    pub(crate) fn preview<R>(
        &mut self,
        channel: u16,
        next: &send::Next,
        encode: impl FnOnce(send::Command<'_>) -> R,
    ) -> Result<Option<R>, wire::Fault> {
        let pending = self.methods.claim(channel, next)?;
        Ok(pending.preview(|item| match item {
            send::Dispatch::Consumer(key) => {
                let tag = self.consumers.read().tag(key)?;
                Some(encode(send::Command::Cancel(tag)))
            }
            send::Dispatch::Orphan(tag) => {
                Some(encode(send::Command::Cancel(short::Short::from(tag))))
            }
            send::Dispatch::Close => Some(encode(send::Command::Close)),
        }))
    }

    pub(crate) fn try_prepare(
        self,
        channel: u16,
        next: send::Next,
    ) -> Result<send::Prepared<'a, 'b, 'd>, wire::Fault> {
        let pending = self.methods.claim(channel, &next)?;
        Ok(send::Prepared::new(self.phase, pending))
    }

    pub(crate) fn replay(
        &mut self,
        intent: replays::Action<'d, consumer::Key, ID>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        ids: &'b resident::Pool,
    ) -> Result<(), failure::Error> {
        let expect = match &intent {
            replays::Action::Plain(kind) => *kind,
            replays::Action::Queue(_) => method::Kind::QueueDeclare,
            replays::Action::Consumer(_) => method::Kind::Consume,
        };
        let next = match (*self.phase, expect) {
            (channel::Phase::Closed, method::Kind::Open) => Some(channel::Phase::Opening),
            (channel::Phase::Open, _) => None,
            (phase, _) => {
                let phase = match phase {
                    channel::Phase::Closed => wire::ChanPhase::Closed,
                    channel::Phase::Opening => wire::ChanPhase::Opening,
                    channel::Phase::Open => wire::ChanPhase::Open,
                    channel::Phase::Closing => wire::ChanPhase::Closing,
                };
                return Err(failure::Error::Protocol(wire::Fault::ReplayPhase {
                    method: expect,
                    phase,
                }));
            }
        };
        match intent {
            replays::Action::Queue(token) => {
                let record = topo
                    .redeclare(token.key())
                    .map_err(failure::Error::Protocol)?;
                let permit = self.methods.replay(expect)?;
                let ready = if token.is_server() {
                    flight::Prepared::Refresh(topo.refresh(record, ids)?)
                } else {
                    drop(record);
                    flight::Prepared::Work(topology::Work::QueueRedeclare {
                        key: token.key(),
                        update_current: false,
                    })
                };
                permit.commit(ready);
                topo.track();
            }
            replays::Action::Plain(_) => {
                self.methods.replay(expect)?.commit(flight::Prepared::None);
            }
            replays::Action::Consumer(token) => {
                let permit = self.methods.replay(expect)?;
                let key = self.consumers.write().begin_replay(token)?;
                permit.commit(flight::Prepared::ConsumerReplay(key));
                topo.track();
            }
        }
        if let Some(next) = next {
            *self.phase = next;
        }
        Ok(())
    }
}

impl<'c, 'b, 'd, const ID: u8> Permit<'_, 'c, 'b, 'd, ID> {
    pub(crate) fn commit(
        self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        ids: &'b resident::Pool,
        admit: work::Admit<'c, 'b, 'd, ID>,
    ) -> Result<(), failure::Error> {
        match admit {
            work::Admit::Consumer(spec) => {
                let identity = if spec.tag().is_empty() {
                    Some(
                        ids.try_writer()
                            .ok_or(failure::Error::Capacity(wire::Capacity::TopologyIdentity))?,
                    )
                } else {
                    None
                };
                let key = self.consumers.write().insert(spec)?;
                self.method
                    .commit(flight::Prepared::ConsumerNew { key, identity });
                topo.track();
                Ok(())
            }
            work::Admit::Direct(identity) => self.commit_work(topo, ids, identity, None),
            work::Admit::Topology { identity, work } => {
                self.commit_work(topo, ids, identity, Some(work))
            }
        }
    }

    fn commit_work(
        self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        ids: &'b resident::Pool,
        identity: need::IdentityPlan,
        work: Option<topology::Work<'c, 'b, 'd, ID>>,
    ) -> Result<(), failure::Error> {
        let track = work.is_some();
        let writer = match identity {
            need::IdentityPlan::None => Ok(None),
            need::IdentityPlan::Fresh => ids
                .try_writer()
                .map(Some)
                .ok_or(failure::Error::Capacity(wire::Capacity::TopologyIdentity)),
        };
        let writer = match writer {
            Ok(writer) => writer,
            Err(error) => {
                if let Some(work) = work {
                    work.undo(topo);
                }
                return Err(error);
            }
        };
        let ready = match writer {
            Some(writer) => flight::Prepared::Identity { writer, work },
            None => work.map_or(flight::Prepared::None, flight::Prepared::Work),
        };
        self.method.commit(ready);
        if track {
            topo.track();
        }
        Ok(())
    }
}

impl<'c, 'b, 'a, 'd, const ID: u8> Publish<'a, 'c, 'b, 'd, ID> {
    pub(crate) fn is_waiting(&self) -> bool {
        matches!(
            self.methods.head(),
            methods::Head::Waiting | methods::Head::Sent(method::Kind::Confirm)
        )
    }

    pub(crate) fn flight(self) -> &'a mut streams::Flight {
        self.publishing
    }

    pub(crate) fn split(self) -> (Confirm<'a, 'c, 'b, 'd>, &'a mut streams::Flight) {
        (
            Confirm {
                control: self.control,
                confirm: self.confirm,
            },
            self.publishing,
        )
    }
}

impl<'c, 'b, 'a, 'd> Confirm<'a, 'c, 'b, 'd> {
    pub(crate) fn enable(&mut self) {
        self.control.enable();
    }

    pub(crate) fn try_reserve<R>(
        self,
        admit: impl FnOnce(lanes::Slot<lanes::Confirm>, confirms::Key<'d>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        if !self.control.has_confirms() {
            return Err(failure::Error::Protocol(
                wire::Fault::ConfirmPublishDisabled,
            ));
        }
        self.confirm.try_reserve(admit)
    }

    pub(crate) fn poll(
        &mut self,
        key: confirms::Key<'d>,
        wake: wake::Waker<'d>,
    ) -> Option<Result<Option<protocol::Confirmation>, terminal::Outcome<'c, 'b, 'd>>> {
        if !self.control.has_confirms() {
            return None;
        }
        self.confirm.poll(key, wake)
    }

    pub(crate) fn complete(
        &mut self,
        tag: u64,
        many: bool,
        result: protocol::Confirmation,
    ) -> Result<(), failure::Error> {
        if !self.control.has_confirms() {
            return Err(failure::Error::Protocol(wire::Fault::ConfirmAckDisabled {
                tag,
                multiple: many,
            }));
        }
        self.confirm.complete(tag, many, result)
    }

    pub(crate) fn reap(&mut self, id: num::NonZero<u64>) -> confirms::Reap {
        if !self.control.has_confirms() {
            return confirms::Reap::Missing;
        }
        self.confirm.reap(id)
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Method<'_, '_, '_, '_, 0>>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Permit<'_, '_, '_, '_, 0>>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Publish<'_, '_, '_, '_, 0>>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Content<'_, '_, '_, '_, '_, 0>>() == 7 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Result<delivery::Route<'_, '_, '_, '_>, resource::Slot>>()
            == mem::size_of::<Option<delivery::Route<'_, '_, '_, '_>>>()
    );
    assert!(
        mem::size_of::<Result<returns::Vacant<'_, '_, '_, '_>, resource::Slot>>()
            == mem::size_of::<Option<returns::Vacant<'_, '_, '_, '_>>>()
    );
};
