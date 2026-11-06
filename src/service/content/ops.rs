use std::num;

use dope::core::driver::route::KeyTag;
use dope::manifold::client::{self, close};
use dope::manifold::service::bindings::endpoint::State;
use dope::net::link::egress;
use hold::scope;
use o3::base::cell::{local, region};
use o3::buffer;
use o3::buffer::{hold, pool};

use crate::failure::recovery;
use crate::failure::wire::Fault;
use crate::protocol::method::conn::{Open, Start, StartOk, Tune, TuneOk};
use crate::protocol::wire::frame;
use crate::protocol::{method, wire};
use crate::service::content::wait::{Attempt, Blocked};
use crate::service::content::{payload, pending};
use crate::service::logic::session::SessionOps as _;
use crate::service::method::commit;
use crate::state::channel::{Freed, Phase, consumer, methods};
use crate::state::conn::{self, phase, status};
use crate::state::{channels, epoch, shared};
use crate::{config, failure, protocol, service};

pub(crate) enum Input<'d, const ID: u8> {
    Committed,
    Blocked(Blocked<'d, ID>),
}

pub(in crate::service) enum FailureOwner<'d, const ID: u8> {
    Immediate(epoch::Id<'d, ID>),
    Reply(commit::Commit<'d, ID>),
}

impl<'d, const ID: u8> FailureOwner<'d, ID> {
    const fn epoch(&self) -> epoch::Id<'d, ID> {
        match self {
            Self::Immediate(epoch) => *epoch,
            Self::Reply(reply) => reply.epoch(),
        }
    }

    fn directive(self, retire: status::Retirement, reason: close::Reason) -> close::Directive {
        match self {
            Self::Immediate(_) => retire.abort(reason),
            Self::Reply(_) => retire.drain(reason),
        }
    }
}

pub(crate) trait Ops<'o, 'r, 'c, 'b, 'd, const ID: u8> {
    fn wake_route(&self, epoch: epoch::Id<'d, ID>, freed: Freed) -> Result<(), failure::Error>;

    fn apply_next(
        &mut self,
        lease: channels::Lease<'d, ID>,
        next: methods::Next<'b, 'd>,
        epoch: epoch::Id<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error>;

    fn complete<C: method::Complete<'b, 'd, consumer::Key>>(
        &mut self,
        lease: channels::Lease<'d, ID>,
        done: C,
        epoch: epoch::Id<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error>
    where
        protocol::Response<'b, 'd, consumer::Key>:
            From<<C::Reply as method::Reply>::Value<'b, 'd, consumer::Key>>;

    fn start(
        &mut self,
        start: &Start,
        state: &mut conn::Transport,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error>;

    fn tune(
        &mut self,
        tune: &Tune,
        state: &mut conn::Transport,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error>;

    fn header<'input, R: buffer::Cursor>(
        &mut self,
        channel: frame::Channel,
        payload: scope::Deferred<'input, 'd, R>,
        attempt: Attempt<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Input<'d, ID>, failure::Error>;
    fn body<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        channel: frame::Channel,
        payload: scope::Deferred<'input, 'd, R>,
        attempt: Attempt<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Input<'d, ID>, failure::Error>;
    fn fail_session<P: recovery::Policy>(
        &mut self,
        close: &mut impl FnMut(close::Directive),
        owner: FailureOwner<'d, ID>,
        failure: recovery::Fault<'b, 'd, P>,
        region: &mut region::Token<region::Identity<'d>>,
    );
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Ops<'o, 'r, 'c, 'b, 'd, ID>
    for State<'o, 'd, service::Port<'r, 'c, 'b, 'd, ID>, service::Logic<'o, 'd, ID>, KeyTag<ID>, 1>
{
    fn wake_route(&self, epoch: epoch::Id<'d, ID>, freed: Freed) -> Result<(), failure::Error> {
        match freed {
            Freed::Available => Ok(()),
            Freed::Route => self.port().content.wake_route(epoch.token()),
        }
    }

    fn complete<C: method::Complete<'b, 'd, consumer::Key>>(
        &mut self,
        lease: channels::Lease<'d, ID>,
        done: C,
        epoch: epoch::Id<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error>
    where
        protocol::Response<'b, 'd, consumer::Key>:
            From<<C::Reply as method::Reply>::Value<'b, 'd, consumer::Key>>,
    {
        let done: <C::Reply as method::Reply>::Value<'b, 'd, consumer::Key> = done.into();
        let response = protocol::Response::from(done);
        let stores = self.port().content.stores();
        let done = self.with_shared(region, |shared| {
            shared.routes.project(lease, |view| {
                (
                    view.index.lane(),
                    view.channel
                        .method_turn(stores)
                        .complete(view.topology, response),
                )
            })
        });
        let (index, done) = done.ok_or_else(|| {
            failure::Error::Protocol(Fault::ChannelUnknown {
                channel: lease.id(),
            })
        })?;
        let (done, deletion) = done?;
        if let Some(deletion) = deletion {
            self.with_shared(region, |shared| {
                shared.routes.for_each(|channel, topology| {
                    topology.deleted(&deletion);
                    channel
                        .consumers()
                        .forget_queue(deletion.queue_key(), deletion.name().into());
                });
            });
        }
        self.apply_next(lease, done, epoch, region)?;
        self.port().admission().wake_channel(index);
        Ok(())
    }

    fn apply_next(
        &mut self,
        lease: channels::Lease<'d, ID>,
        next: methods::Next<'b, 'd>,
        epoch: epoch::Id<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error> {
        match next {
            methods::Next::Idle { freed } => self.wake_route(epoch, freed)?,
            methods::Next::Resume { next, freed } => {
                self.wake_route(epoch, freed)?;
                let _ = self
                    .port()
                    .control()
                    .write(epoch, lease)
                    .send(local::Access::new(region), next)?;
            }
            methods::Next::Replay { freed } => {
                self.wake_route(epoch, freed)?;
                self.with_shared(region, |shared| shared.recovery.resume(lease))?;
                self.port().coordination().waiters().wake_flow();
            }
            methods::Next::Close { freed } => {
                use crate::adapter::channel::Lease;

                self.wake_route(epoch, freed)?;
                self.port()
                    .control()
                    .close(local::Access::new(region), Lease::from_raw(lease));
            }
            methods::Next::Cancel { cancel, freed } => {
                self.wake_route(epoch, freed)?;
                self.port()
                    .control()
                    .cancel(local::Access::new(region), lease.id(), cancel);
            }
            methods::Next::ReplayCancel { cancel, freed } => {
                self.wake_route(epoch, freed)?;
                self.port()
                    .control()
                    .replay(local::Access::new(region), lease.id(), cancel);
            }
        }
        Ok(())
    }

    fn start(
        &mut self,
        start: &Start,
        state: &mut conn::Transport,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error> {
        let config = self.logic().config;
        if (start.version_major, start.version_minor) != (0, 9) {
            return Err(failure::Error::Version {
                major: start.version_major,
                minor: start.version_minor,
            });
        }
        let auth = config.auth.name();
        if !start
            .mechanisms()
            .split(' ')
            .any(|item| item == auth.value())
        {
            return Err(failure::Error::UnsupportedMechanism);
        }
        if !start
            .locales()
            .split(' ')
            .any(|locale| !locale.is_empty() && locale == config.auth.locale.as_ref())
        {
            return Err(failure::Error::UnsupportedLocale);
        }
        let locale = config.auth.locale.as_short();
        let response = config.auth.response.as_long();
        self.send_method(
            out,
            region,
            0,
            &StartOk {
                client_properties: &config.properties,
                mechanism: auth,
                response,
                locale,
            },
        )?;
        state.phase = phase::Phase::Tune;
        Ok(())
    }

    fn tune(
        &mut self,
        tune: &Tune,
        state: &mut conn::Transport,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), failure::Error> {
        let config = self.logic().config;
        if tune.frame_max != 0 && tune.frame_max < config::Frame::MIN {
            return Err(failure::Error::Protocol(Fault::TuneFrame {
                actual: tune.frame_max,
                min: config::Frame::MIN,
            }));
        }
        let agreed = protocol::Negotiated {
            channel_max: match (config.tune.channel_max, tune.channel_max) {
                (0, server) => server,
                (client, 0) => client,
                (client, server) => client.min(server),
            },
            frame_max: match (config.tune.frame.get(), tune.frame_max) {
                (client, 0) => client,
                (client, server) => client.min(server),
            },
            heartbeat: match (config.tune.heartbeat, tune.heartbeat) {
                (0, server) => server,
                (client, 0) => client,
                (client, server) => client.min(server),
            },
        };
        state.negotiated = agreed;
        self.logic().codec.set_max(agreed.frame_max);
        self.with_shared(region, |shared| shared.connection.set_tune(agreed));
        self.send_pair(
            out,
            region,
            0,
            &TuneOk {
                channel_max: agreed.channel_max,
                frame_max: agreed.frame_max,
                heartbeat: agreed.heartbeat,
            },
            &Open {
                virtual_host: config.auth.virtual_host.as_short(),
            },
        )?;
        state.phase = phase::Phase::Open;
        Ok(())
    }

    fn header<'input, R: buffer::Cursor>(
        &mut self,
        channel: frame::Channel,
        payload: scope::Deferred<'input, 'd, R>,
        attempt: Attempt<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Input<'d, ID>, failure::Error> {
        use wire::content::Draft;

        let channel = channel.get();
        let store = &self.port().content;
        let stores = store.stores();
        let result = self.with_shared(region, |shared| {
            let Some(lease) = shared.connection.input.lease() else {
                Draft::parse(payload)?;
                return Err(pending::Error::Failure(failure::Error::Protocol(
                    Fault::ChannelUnknown { channel },
                )));
            };
            shared
                .routes
                .project(lease, move |view| {
                    if view.channel.protocol().phase == Phase::Closing {
                        return Ok(());
                    }
                    let draft = Draft::parse(payload)?;
                    let size = draft.body_size();
                    view.channel
                        .content(stores)
                        .header(size, store.wait.wake(), |demand| {
                            let budget = store.budget();
                            if size > budget.limit() as u64 {
                                return Err(pending::Error::Failure(
                                    failure::Error::MessageTooLarge {
                                        size,
                                        max: budget.limit(),
                                    },
                                ));
                            }
                            let lease = match num::NonZero::<usize>::new(demand) {
                                Some(demand) => Some(
                                    budget
                                        .try_acquire(demand)
                                        .map_err(|error| store.wait.body_exhausted(error))?,
                                ),
                                None => None,
                            };
                            let pool = store.pool();
                            let body = payload::Payload::new(pool.compact, pool.fragments, lease);
                            let props = match store.compact(draft.into_state()) {
                                Ok(props) => props,
                                Err(error) => {
                                    store.discard(body);
                                    return Err(error);
                                }
                            };
                            let resident = !props.is_empty();
                            Ok((props, resident, body))
                        })
                })
                .ok_or_else(|| failure::Error::Protocol(Fault::ChannelUnknown { channel }))?
        });
        match result {
            Ok(()) => Ok(Input::Committed),
            Err(pending::Error::Retry(resource)) => Ok(Input::Blocked(attempt.block(resource))),
            Err(pending::Error::Failure(error)) => Err(error),
        }
    }

    fn body<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        channel: frame::Channel,
        payload: scope::Deferred<'input, 'd, R>,
        attempt: Attempt<'d, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Input<'d, ID>, failure::Error> {
        let channel = channel.get();
        let policy = self.logic().config.capacity.body.retention;
        let store = &self.port().content;
        let stores = store.stores();
        self.with_shared(region, |shared| {
            let lease = shared.connection.input.lease()?;
            shared.routes.project(lease, move |view| {
                let channel = view.channel;
                if channel.protocol().phase == Phase::Closing {
                    return Ok(Input::Committed);
                }
                let mut content = channel.content(stores);
                let length = payload.len();
                let result = content.body(length, store.wait.wake(), |target| {
                    target.store(payload, policy, store)
                });
                match result {
                    Ok(()) => Ok(Input::Committed),
                    Err(pending::Error::Retry(resource)) => {
                        Ok(Input::Blocked(attempt.block(resource)))
                    }
                    Err(pending::Error::Failure(error)) => Err(error),
                }
            })
        })
        .ok_or_else(|| failure::Error::Protocol(Fault::ChannelUnknown { channel }))?
    }

    fn fail_session<P: recovery::Policy>(
        &mut self,
        close: &mut impl FnMut(close::Directive),
        owner: FailureOwner<'d, ID>,
        failure: recovery::Fault<'b, 'd, P>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let source = owner.epoch();
        let causes = self.port().causes();
        let stores = self.port().content.stores();
        let change = self.with_shared(region, |state| {
            state.fail(source, failure, || causes.reserve(), stores)
        });
        let change = match change {
            shared::Failure::Ignored => {
                close(owner.directive(
                    status::Retirement::Permanent,
                    close::Reason::Local(close::LocalOrigin::Superseded),
                ));
                return;
            }
            shared::Failure::Applied(retire) => retire,
        };
        close(owner.directive(
            change,
            close::Reason::Local(close::LocalOrigin::ResourceFailure),
        ));
        self.port().coordination().waiters().wake_all();
        self.port().admission().wake_all();
    }
}
