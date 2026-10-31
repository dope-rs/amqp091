use dope::core::driver::route::KeyTag;
use dope::manifold::client::{self, close};
use dope::manifold::service::bindings::endpoint::State;
use dope::net::link::egress;
use o3::base::cell::region;
use o3::buffer;
use o3::buffer::hold::scope;
use o3::buffer::pool;

use crate::failure::recovery;
use crate::failure::wire::{Fault, Reply};
use crate::protocol::method::{self, channel, conn};
use crate::protocol::wire::frame;
use crate::service::content::ops::{self, Ops as _};
use crate::service::content::wait::Attempt;
use crate::service::logic::session::SessionOps as _;
use crate::state::channel::consumer::access;
use crate::state::channel::methods::flight;
use crate::state::channel::{Freed, Phase, change};
use crate::state::conn::status::Retirement;
use crate::state::conn::{Transport, phase};
use crate::state::{channels, epoch};
use crate::{failure, protocol, service};

pub(in crate::service) mod commit;
mod gates;
mod pending;

pub(crate) trait Ops<'o, 'r, 'c, 'b, 'd, const ID: u8> {
    fn recv<'input, R: buffer::Cursor + 'input>(
        &mut self,
        channel: u16,
        payload: scope::Deferred<'input, 'd, R>,
        closing: bool,
        attempt: Attempt<'d, ID>,
        parts: &mut service::Turn<'_, '_, 'd, ID>,
    ) -> Result<ops::Input<'d, ID>, failure::Error>;

    fn admit_chan(
        &self,
        channel: frame::Channel,
        kind: Result<method::Kind, failure::Error>,
        state: &Transport,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Option<(gates::Channel<'d, ID>, method::Kind)>, failure::Error>;

    fn recv_conn(
        &mut self,
        server: conn::Event<'_, 'd>,
        epoch: epoch::Id<'d, ID>,
        state: &mut Transport,
        close: &mut impl FnMut(close::Directive),
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error>;

    fn recv_chan(
        &mut self,
        lease: channels::Lease<'d, ID>,
        server: channel::Event<'_, 'd>,
        attempt: Attempt<'d, ID>,
        parts: &mut service::Turn<'_, '_, 'd, ID>,
    ) -> Result<ops::Input<'d, ID>, failure::Error>;
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Ops<'o, 'r, 'c, 'b, 'd, ID>
    for State<'o, 'd, service::Port<'r, 'c, 'b, 'd, ID>, service::Logic<'o, 'd, ID>, KeyTag<ID>, 1>
{
    fn recv<'input, R: buffer::Cursor + 'input>(
        &mut self,
        channel: u16,
        payload: scope::Deferred<'input, 'd, R>,
        closing: bool,
        attempt: Attempt<'d, ID>,
        parts: &mut service::Turn<'_, '_, 'd, ID>,
    ) -> Result<ops::Input<'d, ID>, failure::Error> {
        let budget = self.port().budget();
        match pending::Pending::inspect(channel, payload)? {
            pending::Pending::Connection(pending) => {
                let region = parts.driver.region_token();
                let Some(event) = pending.admit(budget, parts.state.phase, closing)? else {
                    return Ok(ops::Input::Committed);
                };
                let mut close = |next| parts.close.request(next);
                self.recv_conn(
                    event,
                    epoch::Id::new(parts.conn_id),
                    parts.state,
                    &mut close,
                    &mut parts.sink,
                    region,
                )?;
                Ok(ops::Input::Committed)
            }
            pending::Pending::Channel(pending) => {
                let Some((gate, event)) = pending.admit(budget, |channel, kind| {
                    if closing {
                        kind?;
                        return Ok(None);
                    }
                    self.admit_chan(channel, kind, parts.state, parts.driver.region_token())
                })?
                else {
                    return Ok(ops::Input::Committed);
                };
                self.recv_chan(gate.into_lease(), event, attempt, parts)
            }
        }
    }

    fn admit_chan(
        &self,
        channel: frame::Channel,
        kind: Result<method::Kind, failure::Error>,
        state: &Transport,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Option<(gates::Channel<'d, ID>, method::Kind)>, failure::Error> {
        if state.phase != phase::Phase::Ready {
            kind?;
            return Err(failure::Error::Protocol(Fault::ChannelNotReady {
                channel: channel.get(),
                phase: gates::connection_phase(state.phase),
                closing: false,
            }));
        }
        let admit = self.with_shared(region, |shared| {
            let lease = shared.routes.lease(channel.get())?;
            shared.routes.project(lease, |view| {
                let state = view.channel.protocol();
                (lease, state.phase, state.expected, state.inbound_idle)
            })
        });
        let Some((lease, phase, expected, idle)) = admit else {
            kind?;
            return Err(failure::Error::Protocol(Fault::ChannelUnknown {
                channel: channel.get(),
            }));
        };
        let closing = phase == Phase::Closing;
        let close = matches!(&kind, Ok(method::Kind::Close | method::Kind::CloseOk));
        if closing && !close && !matches!(&kind, Ok(kind) if expected == Some(*kind)) {
            return Ok(None);
        }
        if !idle && !(closing && close) {
            kind?;
            return Err(failure::Error::Protocol(Fault::ContentInterrupted));
        }
        let kind = kind?;
        gates::Channel::check(kind, phase, expected, lease).map(|gate| Some((gate, kind)))
    }

    fn recv_conn(
        &mut self,
        server: conn::Event<'_, 'd>,
        epoch: epoch::Id<'d, ID>,
        state: &mut Transport,
        close: &mut impl FnMut(close::Directive),
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error> {
        use method::conn::{CloseOk, SecureOk};

        let config = self.logic().config;
        match server {
            conn::Event::Start(start) => self.start(&start, state, out, region),
            conn::Event::Secure(_) => self.send_method(
                out,
                region,
                0,
                &SecureOk {
                    response: config.auth.response.as_long(),
                },
            ),
            conn::Event::Tune(tune) => self.tune(&tune, state, out, region),
            conn::Event::Open(_) => {
                state.phase = phase::Phase::Ready;
                self.with_shared(region, |shared| shared.mark_open(epoch))?;
                self.port().coordination().waiters().wake_state();
                Ok(())
            }
            conn::Event::Close(remote) => {
                let failure = recovery::Fault::connection(failure::Error::Server {
                    reply_code: remote.reply_code,
                    reply_text: remote.reply_text.into(),
                    class_id: remote.class_id,
                    method_id: remote.method_id,
                });
                // The broker's cause remains primary if its reply cannot be queued.
                let owner = match self.send_method(out, region, 0, &CloseOk) {
                    Ok(()) => ops::FailureOwner::Reply(commit::Commit::new(epoch)),
                    Err(_) => ops::FailureOwner::Immediate(epoch),
                };
                self.fail_session(close, owner, failure, region);
                Ok(())
            }
            conn::Event::CloseOk(_) => {
                let cause =
                    self.with_shared(region, |shared| shared.connection.finish_close(epoch))?;
                let reason = if matches!(cause.error(), failure::Error::Closed) {
                    close::Reason::Local(close::LocalOrigin::ApiRequest)
                } else {
                    close::Reason::Protocol
                };
                close(Retirement::Permanent.abort(reason));
                let stores = self.port().content.stores();
                let freed = self.with_shared(region, |shared| {
                    let mut freed = Freed::Available;
                    shared.routes.for_each(|c, topology| {
                        freed = freed.merge(c.change(
                            topology,
                            stores,
                            change::Event::TransportEnded { cause: &cause },
                        ));
                    });
                    freed
                });
                self.wake_route(epoch, freed)?;
                self.port().coordination().waiters().wake_closed();
                Ok(())
            }
            conn::Event::Blocked(b) => {
                self.with_shared(region, |shared| shared.connection.block(b.reason));
                Ok(())
            }
            conn::Event::Unblocked(_) => {
                self.with_shared(region, |shared| shared.connection.unblock());
                self.port().coordination().waiters().wake_flow();
                Ok(())
            }
        }
    }

    fn recv_chan(
        &mut self,
        lease: channels::Lease<'d, ID>,
        server: channel::Event<'_, 'd>,
        attempt: Attempt<'d, ID>,
        parts: &mut service::Turn<'_, '_, 'd, ID>,
    ) -> Result<ops::Input<'d, ID>, failure::Error> {
        let epoch = epoch::Id::new(parts.conn_id);
        let out = &mut parts.sink;
        let region = parts.driver.region_token();
        let mut request = |next| parts.close.request(next);
        let close = &mut request;

        use method::channel::{CloseOk, FlowOk};

        let store = &self.port().content;
        let stores = store.stores();
        let channel = lease.id();
        let done = match server {
            channel::Event::Open(ok) => {
                let freed = self
                    .with_shared(region, |shared| {
                        shared.routes.project(lease, |view| {
                            view.channel
                                .change(view.topology, stores, change::Event::Reopened)
                        })
                    })
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                self.wake_route(epoch, freed)?;
                self.complete(lease, ok, epoch, region)?;
                Ok(())
            }
            channel::Event::Flow(f) => {
                self.send_method(out, region, channel, &FlowOk { active: f.active })?;
                self.with_lease(region, lease, |c| c.control().set_flow(f.active))
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                if f.active {
                    self.port().coordination().waiters().wake_flow();
                }
                Ok(())
            }
            channel::Event::FlowOk(flow) => Err(failure::Error::UnexpectedFlowOk {
                active: flow.active,
            }),
            channel::Event::Close(remote) => {
                let err = failure::Error::Server {
                    reply_code: remote.reply_code,
                    reply_text: remote.reply_text.into(),
                    class_id: remote.class_id,
                    method_id: remote.method_id,
                };
                let replay = self
                    .with_shared(region, |shared| {
                        shared
                            .routes
                            .project(lease, |view| view.channel.has_replay(view.topology))
                    })
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                // The broker's cause remains primary if its reply cannot be queued.
                let owner = match self.send_method(out, region, channel, &CloseOk) {
                    Ok(()) => ops::FailureOwner::Reply(commit::Commit::new(epoch)),
                    Err(_) => ops::FailureOwner::Immediate(epoch),
                };
                if replay && err.is_not_found() {
                    self.fail_session(close, owner, recovery::Fault::channel(err), region);
                } else {
                    self.fail_session(close, owner, recovery::Fault::terminal(err), region);
                }
                Ok(())
            }
            channel::Event::CloseOk(ok) => {
                let (freed, done) = self
                    .with_shared(region, |shared| {
                        shared.routes.project(lease, |view| {
                            let freed = view.channel.change(
                                view.topology,
                                stores,
                                change::Event::ChannelClosed,
                            );
                            let mut turn = view.channel.method_turn(stores);
                            let done = turn.complete(
                                view.topology,
                                <CloseOk as method::Reply>::KIND,
                                protocol::Response::from(ok),
                            );
                            (freed, done)
                        })
                    })
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                self.wake_route(epoch, freed)?;
                let (done, _) = done?;
                self.apply_next(lease, done, epoch, region)?;
                self.with_shared(region, |shared| {
                    shared.routes.project(lease, |view| view.topology.clear())
                })
                .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                self.with_shared(region, |shared| shared.routes.collect(lease));
                Ok(())
            }
            channel::Event::ExchangeDeclare(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::ExchangeDelete(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::ExchangeBind(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::ExchangeUnbind(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::QueueDeclare(view) => {
                let name = view.try_queue()?;
                let queue = self
                    .with_shared(region, |shared| {
                        shared.routes.project(lease, |route| {
                            route
                                .channel
                                .method_turn(stores)
                                .retain_queue(route.topology, name)
                        })
                    })
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                match queue {
                    Ok(queue) => self.complete(lease, view.retain(queue), epoch, region),
                    Err(flight::Fault::Mismatch(mismatch)) => {
                        let error = failure::Error::QueueName {
                            expected: mismatch.into_name(),
                            actual: view.try_name(self.port().budget())?,
                        };
                        self.start_close(close, epoch, error, Reply::Channel, out, region);
                        return Ok(ops::Input::Committed);
                    }
                    Err(flight::Fault::Protocol(error)) => Err(failure::Error::Protocol(error)),
                    Err(flight::Fault::Retain(error)) => Err(failure::Error::Retain(error)),
                }
            }
            channel::Event::QueueBind(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::QueueUnbind(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::QueuePurge(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::QueueDelete(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::Qos(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::Consume(view) => {
                let tag = view.tag();
                let done = self.with_shared(region, |shared| {
                    shared.routes.project(lease, |route| {
                        (
                            route.index.lane(),
                            route.channel.method_turn(stores).consume(tag),
                        )
                    })
                });
                let (index, next) =
                    done.ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                let next = match next {
                    Ok(next) => next,
                    Err(access::Fault::Mismatch(mismatch)) => {
                        let error = mismatch.try_error(|| view.try_retain(self.port().budget()))?;
                        self.start_close(close, epoch, error, Reply::Channel, out, region);
                        return Ok(ops::Input::Committed);
                    }
                    Err(access::Fault::Protocol(error)) => {
                        return Err(failure::Error::Protocol(error));
                    }
                    Err(access::Fault::Retain(error)) => {
                        return Err(failure::Error::Retain(error));
                    }
                };
                self.apply_next(lease, next, epoch, region)?;
                self.port().admission().wake_channel(index);
                self.port().coordination().waiters().wake_flow();
                Ok(())
            }
            channel::Event::CancelOk(ok) => {
                let done = self.with_shared(region, |shared| {
                    shared.routes.project(lease, |route| {
                        let mut turn = route.channel.method_turn(stores);
                        (route.index.lane(), turn.cancel(route.topology, ok))
                    })
                });
                let (index, done) =
                    done.ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                match done {
                    Ok(next) => {
                        self.apply_next(lease, next, epoch, region)?;
                        self.port().admission().wake_channel(index);
                    }
                    Err(error) => {
                        self.start_close(close, epoch, error, Reply::Channel, out, region);
                    }
                }
                Ok(())
            }
            channel::Event::Cancel(c_msg) => {
                let tag = c_msg.consumer_tag();
                let freed = self
                    .with_lease(region, lease, |c| c.content(stores).cancel(tag.value()))
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                self.wake_route(epoch, freed)?;
                Ok(())
            }
            channel::Event::Recover(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::Ack(a) => self
                .with_lease(region, lease, |c| {
                    c.confirm()
                        .complete(a.delivery_tag, a.multiple, protocol::Confirmation::Ack)
                })
                .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?,
            channel::Event::Nack(n) => self
                .with_lease(region, lease, |c| {
                    c.confirm().complete(
                        n.delivery_tag,
                        n.multiple,
                        protocol::Confirmation::Nack { requeue: n.requeue },
                    )
                })
                .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?,
            channel::Event::Deliver(delivery) => {
                return self
                    .with_lease(region, lease, |channel| {
                        let mut content = channel.content(stores);
                        let admit = content
                            .delivery(delivery.consumer_tag())
                            .and_then(|route| store.pool().deliver(delivery, route));
                        match admit {
                            Ok(entry) => content.begin(entry).map(|()| ops::Input::Committed),
                            Err(resource) => {
                                Ok(ops::Input::Blocked(attempt.block(resource.into())))
                            }
                        }
                    })
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
            }
            channel::Event::Return(returned) => {
                return self
                    .with_lease(region, lease, |channel| {
                        let mut content = channel.content(stores);
                        let admit = content
                            .reserve()
                            .and_then(|route| store.pool().accept(returned, route));
                        match admit {
                            Ok(entry) => content.begin(entry).map(|()| ops::Input::Committed),
                            Err(resource) => {
                                Ok(ops::Input::Blocked(attempt.block(resource.into())))
                            }
                        }
                    })
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
            }
            channel::Event::Confirm(ok) => {
                self.with_lease(region, lease, |c| c.confirm().enable())
                    .ok_or(failure::Error::Protocol(Fault::ChannelUnknown { channel }))?;
                self.complete(lease, ok, epoch, region)
            }
            channel::Event::TxSelect(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::TxCommit(ok) => self.complete(lease, ok, epoch, region),
            channel::Event::TxRollback(ok) => self.complete(lease, ok, epoch, region),
        };
        done.map(|()| ops::Input::Committed)
    }
}
