use core::convert::Infallible;

use dope::core::Errno;
use dope::core::driver::route::KeyTag;
use dope::core::driver::{self};
use o3::buffer::pool;
use o3::buffer::write::consume::scope;
use policy::output;
mod drain;
mod replay;
pub(in crate::service) mod session;

use std::time;

use bindings::endpoint::State;
use dope::core::driver::schedule;
use dope::core::driver::schedule::app::Work;
use dope::core::driver::schedule::clock;
use dope::core::io::socket;
use dope::manifold::client::app::admit::Registered;
use dope::manifold::client::app::connect::Full;
use dope::manifold::client::app::failure::Controlled;
use dope::manifold::client::app::{self, request};
use dope::manifold::client::conn::{Activation, Id, Transmit};
use dope::manifold::client::session::app::drain::Ready;
use dope::manifold::client::session::codec::{self, mode};
use dope::manifold::client::session::{policy, protocol, response, turns};
use dope::manifold::client::{attempt, close, port};
use dope::manifold::service::bindings;
use dope::net::link::egress;
use dope::net::link::pool::change::opening;
use o3::base::Call;
use o3::base::cell::{local, region};
use o3::base::time::Positive;
use o3::buffer::{self, hold};
use policy::admit;
use port::error;

use crate::config;
use crate::failure::{self, recovery};
use crate::protocol::wire::frame;
use crate::service::content::ops::{FailureOwner, Input, Ops as _};
use crate::service::content::wait::Blocked;
use crate::service::logic::session::SessionOps as _;
use crate::service::{self, Port, runtime};
use crate::state::{channel, conn, epoch, shared};
use dope::manifold::client::session::Blocking;

#[doc(hidden)]
pub struct Logic<'o, 'd, const ID: u8> {
    pub(in crate::service) codec: frame::Codec,
    pub(in crate::service) config: &'o config::Config,
    heartbeat: runtime::Heartbeat<'d, ID>,
}

impl<'o, 'd, const ID: u8> Logic<'o, 'd, ID> {
    pub(in crate::service) fn new<'r, 'c, 'b>(port: &'o Port<'r, 'c, 'b, 'd, ID>) -> Self {
        let config = port.config();
        Self {
            codec: frame::Codec::new(config.tune.frame.get()),
            config,
            heartbeat: runtime::Heartbeat::new(),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    protocol::Spec<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    type Codec = frame::Codec;
    type Connect = Full;
    type ConnState = conn::Transport;
    type Failure = Controlled;
    type Mode = mode::Discard;
    type Requests = request::Enabled;
    type Response = response::Immediate;
    type Receipt = conn::phase::Phase;
    type Send = pool::Cursor;

    fn codec<'a>(
        owner: &'a State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
    ) -> &'a Self::Codec {
        &owner.logic().codec
    }

    fn receipt(
        _: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        _: Id<'d, KeyTag<ID>>,
        state: &Self::ConnState,
        _: &region::Token<region::Identity<'d>>,
    ) -> Self::Receipt {
        state.phase
    }

    fn received<'e>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        receipt: Self::Receipt,
        has_prefix: bool,
        context: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) {
        if receipt == conn::phase::Phase::Ready {
            return;
        }
        context.state.received = receipt;
        if !has_prefix {
            let epoch = epoch::Id::new(context.conn_id);
            owner.with_shared(context.region(), |shared| {
                if let conn::Input::Unbound(_, phase) = &mut shared.connection.input
                    && shared
                        .connection
                        .lifecycle
                        .status()
                        .can_accept(epoch, conn::status::Input::Frame)
                {
                    *phase = receipt;
                }
            });
        }
    }

    fn stalled<'e>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        cause: &dope::manifold::app::receive::Failure,
        context: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) -> dope::manifold::client::session::Blocking {
        use dope::manifold::app::receive::Failure;
        let resource = match cause {
            Failure::Staging(error) => owner.port().content.retry_staging(error),
            _ => None,
        };
        let resource = match resource {
            Some(resource) => resource,
            _ => return Blocking::Unsupported,
        };
        let attempt = owner
            .port()
            .content
            .attempt(context.conn_id, context.wake_target());
        block(owner, attempt.block(resource), context)
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    protocol::Plain<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    fn parse<'id, 'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        input: codec::Input<'id, 'input, 'd, R>,
        context: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) -> Result<codec::Parse<'id, frame::Inbound<'input, 'd, R>>, scope::Error>
    where
        'd: 'input,
    {
        let bytes = input.as_slice();
        let parsed = codec::Stateless::parse(&owner.logic().codec, input);
        let epoch = epoch::Id::new(context.conn_id);
        let mut parts = context.parts();
        owner.with_shared(parts.driver.region_token(), |shared| {
            let status = shared.connection.lifecycle.status();
            if !status.can_accept(epoch, conn::status::Input::Frame) {
                return;
            }
            let bound = match shared.connection.input {
                conn::Input::Held(_) => return,
                conn::Input::Partial(bound) => bound,
                input => {
                    let generation = match input {
                        conn::Input::Unbound(generation, _) => generation,
                        _ => shared.routes.generation(),
                    };
                    let phase = match &shared.connection.input {
                        conn::Input::Unbound(_, phase) => phase,
                        _ => &parts.state.received,
                    };
                    let Some([tag, high, low]) = bytes.first_chunk::<3>().copied() else {
                        shared.connection.input = conn::Input::Unbound(generation, *phase);
                        return;
                    };
                    let channel = u16::from_be_bytes([high, low]);
                    if channel == 0 {
                        conn::Owner::Connection {
                            phase: *phase,
                            closing: status.is_closing(),
                        }
                    } else {
                        let lease = shared.routes.lease(channel).filter(|lease| {
                            generation.is_some_and(|generation| generation.admits(*lease))
                        });
                        match lease {
                            Some(lease) => {
                                let (sent, published) = if tag == frame::Type::METHOD {
                                    shared
                                        .routes
                                        .project(lease, |view| {
                                            let sent = view.channel.method_state().methods.sent();
                                            (sent, view.channel.publish().published())
                                        })
                                        .unwrap_or((None, 0))
                                } else {
                                    (None, 0)
                                };
                                conn::Owner::Channel {
                                    lease,
                                    sent,
                                    published,
                                }
                            }
                            None => conn::Owner::Unknown,
                        }
                    }
                }
            };
            shared.connection.input = conn::Input::Held(bound);
        });
        parsed
    }

    fn reject<'e>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        cause: scope::Error,
        context: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) -> response::Direct {
        let mut parts = context.parts();
        let epoch = epoch::Id::new(parts.conn_id);
        let mut close = |plan| parts.close.request(plan);
        owner.fail_session(
            &mut close,
            FailureOwner::Immediate(epoch),
            recovery::Fault::session(failure::Error::Input(cause)),
            parts.driver.region_token(),
        );
        response::Direct::Committed
    }
    fn deliver<'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        head: frame::Inbound<'input, 'd, R>,
        context: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        let conn = context.conn_id;
        let target = context.wake_target();
        let attempt = owner.port().content.attempt(conn, target);
        let input = {
            let mut parts = context.parts();
            owner.respond(head, attempt, &mut parts)
        };
        let delivery = match input {
            Input::Committed => response::Direct::Committed,
            Input::Blocked(blocked) => match block(owner, blocked, context) {
                Blocking::Registered => response::Direct::RetryInput,
                Blocking::Unsupported => response::Direct::Committed,
            },
        };
        if delivery == response::Direct::Committed {
            let received = context.state.received;
            owner.with_shared(context.region(), |shared| {
                if shared
                    .connection
                    .lifecycle
                    .status()
                    .can_accept(epoch::Id::new(conn), conn::status::Input::Frame)
                {
                    shared.connection.input =
                        conn::Input::Unbound(shared.routes.generation(), received);
                }
            });
        }
        delivery
    }
}

fn block<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, 'e, const ID: u8>(
    owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Logic<'o, 'd, ID>, KeyTag<ID>, 1>,
    blocked: Blocked<'d, ID>,
    context: &mut turns::Context<'_, 'e, 'd, conn::Transport, pool::Cursor, KeyTag<ID>>,
) -> Blocking {
    match owner.port().content.register(blocked) {
        Ok(()) => Blocking::Registered,
        Err(error) => {
            let mut parts = context.parts();
            let mut close = |plan| parts.close.request(plan);
            owner.fail_session(
                &mut close,
                FailureOwner::Immediate(epoch::Id::new(parts.conn_id)),
                recovery::Fault::terminal(error.into()),
                parts.driver.region_token(),
            );
            Blocking::Unsupported
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    admit::Register<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    fn register(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        active: &Activation<'_, 'd, KeyTag<ID>>,
    ) -> app::Admission {
        match owner.root().domain().transport.try_activate(active) {
            Ok(()) => app::Admission::Attached,
            Err(error::Open::Unknown | error::Open::Exhausted) => app::Admission::Rejected(
                close::Directive::cut_final(close::Reason::Capacity(close::Capacity::Transport)),
            ),
            Err(error::Open::Occupied) => app::Admission::Rejected(close::Directive::cut_final(
                close::Reason::Local(close::LocalOrigin::ResourceFailure),
            )),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    policy::Output<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    fn sent() -> impl for<'a, 'x> Call<(
        &'a mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        Id<'d, KeyTag<ID>>,
        egress::Pending,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<_>::Ready(
            |owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>| {
                service::Admission::new(owner.root().domain()).sync();
            },
        )
    }

    fn flush() -> Option<
        impl for<'a, 'x, 'y, 'z> Call<
            output::BoundArgs<
                'a,
                'x,
                'y,
                'z,
                'o,
                'd,
                Port<'r, 'c, 'b, 'd, ID>,
                Self,
                KeyTag<ID>,
                1,
            >,
        >,
    > {
        Some(
            |(owner, context, input): output::BoundArgs<
                '_,
                '_,
                '_,
                '_,
                'o,
                'd,
                Port<'r, 'c, 'b, 'd, ID>,
                Self,
                KeyTag<ID>,
                1,
            >| {
                let epoch = epoch::Id::new(context.conn_id);
                let received = context.state.received;
                let ready = owner.with_shared(context.region(), |shared| {
                    if !shared
                        .connection
                        .lifecycle
                        .status()
                        .can_accept(epoch, conn::status::Input::Frame)
                    {
                        return false;
                    }
                    shared
                        .connection
                        .input
                        .settle(input, shared.routes.generation(), received)
                });
                if ready {
                    owner.port().admission().wake_credit();
                    owner.port().coordination().waiters().wake_flow();
                }
                service::Admission::new(owner.root().domain()).sync();
            },
        )
    }

    fn cancel() -> impl for<'a, 'x> Call<(
        &'a State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        Self::Send,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<Infallible, _>::Owner(
            |(owner, payload): (
                &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
                Self::Send,
            )| {
                drop(payload);
                service::Admission::new(owner.root().domain()).sync();
            },
        )
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    policy::Connected<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    type Activation = Registered;

    fn connect<'e>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        _: attempt::Completion<'_, 'd, KeyTag<ID>>,
        _: socket::Addr,
        mut ctx: turns::Startup<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) {
        let epoch = epoch::Id::new(ctx.connection());
        let until = ctx.lifetime.expires_at();
        let mut close = |plan| ctx.close.request(plan);
        owner.connect(
            epoch,
            ctx.state,
            &mut close,
            &mut ctx.sink,
            until,
            ctx.driver.region_token(),
        );
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    policy::Exhausted<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    fn exhaust(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        _: attempt::Completion<'_, 'd, KeyTag<ID>>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let change = owner.with_shared(driver.region_token(), |shared| shared.reject(Some(cause)));
        if change == conn::Change::Applied {
            owner.port().wake_all();
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    policy::Lifecycle<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    type Drain = Ready;

    fn take_close(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        conn: Id<'d, KeyTag<ID>>,
    ) -> Option<close::Directive> {
        owner.root().domain().transport.take_close(conn)
    }

    fn transmit_failed(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        conn: Id<'d, KeyTag<ID>>,
        failure: Transmit,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) -> Option<Transmit> {
        let source = epoch::Id::new(conn);
        let stores = owner.port().content.stores();
        let change = owner.with_shared(&mut regions.0, |shared| {
            shared.fail(
                source,
                recovery::Fault::session(failure::Error::Transmit(failure)),
                // Preserve the generation's first cause when a later send failure is fatal.
                || None,
                stores,
            )
        });
        match change {
            shared::Failure::Ignored => Some(failure),
            shared::Failure::Applied(_) => {
                owner.port().wake_all();
                None
            }
        }
    }

    fn io_fail<'e>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        errno: Errno,
        context: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, KeyTag<ID>>,
    ) {
        let mut parts = context.parts();
        let epoch = epoch::Id::new(parts.conn_id);
        let mut close = |plan| parts.close.request(plan);
        owner.fail_session(
            &mut close,
            FailureOwner::Immediate(epoch),
            recovery::Fault::session(failure::Error::Receive { errno }),
            parts.driver.region_token(),
        );
    }

    fn clear_send<'turn>(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        conn: Id<'d, KeyTag<ID>>,
        context: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = context.parts();
        owner
            .root()
            .domain()
            .transport
            .retire(conn, work, region, |tagged| {
                let (frame, _) = tagged.into_parts();
                drop(frame);
            })
    }

    fn quarantine(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        conn: Id<'d, KeyTag<ID>>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let region = &mut regions.0;
        let epoch = epoch::Id::new(conn);
        owner.with_shared(region, |shared| shared.retire(epoch, reason.into()));
    }

    fn policy(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        conn: Id<'d, KeyTag<ID>>,
        _: &Self::ConnState,
        _: close::Reason,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> close::Policy {
        let epoch = epoch::Id::new(conn);
        owner.with_shared(region, |shared| {
            shared
                .connection
                .lifecycle
                .status()
                .retirement(epoch)
                .policy()
        })
    }

    fn disconnect(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        context: turns::Retired<
            '_,
            'd,
            Self::ConnState,
            KeyTag<ID>,
            State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        >,
    ) {
        let reason = context.reason();
        let (drained, _, region) = context.into_parts();
        owner.disconnect(epoch::Id::new(drained.connection()), reason, region);
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    policy::Requests<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    fn drain<'e>(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        requests: &mut request::Requests<'_, '_, 'e, 'd, Self::Send, KeyTag<ID>>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let endpoint = owner;
        owner.root().domain().transport.drain_tagged(
            requests,
            |region, auth| match auth {
                None => true,
                Some(lease) => endpoint.with_shared(region, |shared| {
                    match shared
                        .routes
                        .project(lease, |route| route.channel.control().phase)
                    {
                        Some(
                            channel::Phase::Opening
                            | channel::Phase::Open
                            | channel::Phase::Closing,
                        ) => true,
                        Some(channel::Phase::Closed) | None => false,
                    }
                }),
            },
            |rejected| {
                let (frame, _) = rejected.into_parts();
                drop(frame);
            },
        );
        service::Admission::new(endpoint.port()).sync();
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    policy::Scheduling<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, KeyTag<ID>, 1> for Logic<'o, 'd, ID>
{
    fn park<'turn>(
        owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        work: Work<'turn, 'd>,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let endpoint = owner;
        endpoint.port().coordination().waiters().flush(work);
        endpoint.port().admission().flush(work);
        drain::run(endpoint, work, &mut region.0);
        replay::run(endpoint, work, &mut region.0);
        let port = endpoint.port();
        endpoint
            .logic_mut()
            .heartbeat
            .emit(port, work, &mut region.0);
    }

    fn shutdown(owner: &mut State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>) {
        owner.port().wake_all();
    }

    fn progress(
        owner: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        region: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        let endpoint = owner;
        let coord = endpoint.port().coordination();
        if coord.waiters().has_pending()
            || endpoint.port().admission().has_pending()
            || coord.cancellations.has_pending()
        {
            return schedule::Progress::Runnable;
        }
        if endpoint
            .port()
            .session
            .read(local::Read::new(region), |shared| {
                shared.recovery.runnable().is_some() && shared.connection.input.is_ready()
            })
            && endpoint.port().replay_available()
        {
            return schedule::Progress::Runnable;
        }
        owner.logic().heartbeat.progress(endpoint.port(), region)
    }

    fn inbound(
        _: &State<'o, 'd, Port<'r, 'c, 'b, 'd, ID>, Self, KeyTag<ID>, 1>,
        _: Id<'d, KeyTag<ID>>,
        state: &Self::ConnState,
        base: Positive,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        use o3::base::time::Zero;

        use crate::state::conn::phase::Phase;

        if state.phase != Phase::Ready {
            return app::Inbound::Awaiting(base);
        }
        let seconds = u64::from(state.negotiated.heartbeat);
        match Positive::try_new(time::Duration::from_secs(seconds)) {
            Ok(window) => app::Inbound::Awaiting(window),
            Err(Zero) => app::Inbound::Quiescent,
        }
    }
}
