use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use dope::driver::token::Token;
use dope::manifold::connector::source::Dialer;
use dope::manifold::connector::{Connector, Sender};
use dope::manifold::env::Env;
use dope::manifold::timer::Timer;
use dope_fiber::{Context, Fiber, Waker, poll_fn};
use dope_net::Transport;

use crate::content::properties::BasicProperties;
use crate::error::Error;
use crate::method::connection::Close;
use crate::method::core::ClientMethod;
use crate::method::{basic, confirm, exchange, queue, tx};
use crate::protocol::channels::state::ChannelState;
use crate::protocol::connections::shared::Shared;
use crate::protocol::session::Session;
use crate::protocol::session::outbound::Outbound;
use crate::protocol::session::port::AmqpPort;
use crate::protocol::{ExpectMethod, Negotiated};
use crate::runtime::channel::backpressure::{WaitFlowResumed, WaitUnblocked};
use crate::runtime::channel::consumer::Consumer;
use crate::runtime::channel::extract::{Extract, RegisteredConsumer};
use crate::runtime::channel::method::future::MethodFuture;
use crate::runtime::channel::publish::future::PublishConfirmFuture;
use crate::runtime::channel::returns::NextReturn;
use crate::runtime::connection::handshake::Handshake;
use crate::topology::{
    ConsumerSpec, ExchangeBindSpec, ExchangeDecl, QosSpec, QueueBindSpec, QueueDecl, Topology,
};
use crate::wire::field::FieldTable;
use crate::wire::frame::{FrameEncoder, FrameType, OVERHEAD};

pub mod channel;
pub mod connection;
pub mod open;

use self::channel::Channel;
use self::open::OpenChannel;

type HandleMarker<'a, S, E> = PhantomData<(&'a (), fn() -> (S, E))>;

pub struct AmqpHandle<'a, 'd, S, E> {
    port: &'d AmqpPort<'d>,
    marker: HandleMarker<'a, S, E>,
}

impl<S, E> Copy for AmqpHandle<'_, '_, S, E> {}

impl<S, E> Clone for AmqpHandle<'_, '_, S, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, 'd, S, E> AmqpHandle<'a, 'd, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn from_port(port: &'d AmqpPort<'d>) -> Self {
        Self {
            port,
            marker: PhantomData,
        }
    }

    pub fn from_cell(conn: Pin<&Connector<'d, 0, Session<'d>, S, E>>) -> Self {
        Self::from_port(conn.get_ref().session().port())
    }
}

#[derive(Clone, Copy)]
pub(super) struct ConnectorRef<'d> {
    port: &'d AmqpPort<'d>,
}

#[derive(Clone, Copy)]
pub(super) enum Missing {
    Wait,
    Recover,
}

#[derive(Clone, Copy)]
pub(super) struct SessionRef<'d> {
    port: &'d AmqpPort<'d>,
}

impl<'d> SessionRef<'d> {
    pub(super) fn with_shared<R>(&self, f: impl for<'s> FnOnce(&'s mut Shared<'d>) -> R) -> R {
        self.port.with_shared(f)
    }

    pub(super) fn with_channel<R>(
        &self,
        channel: u16,
        f: impl for<'s> FnOnce(&'s mut ChannelState<'d>) -> R,
    ) -> Option<R> {
        self.with_shared(|shared| shared.with_channel_mut(channel, f))
    }
}

pub(super) struct StateRef<'a, 'd> {
    sender: Sender<'a, 'd, Outbound>,
    port: &'d AmqpPort<'d>,
}

impl<'a, 'd> StateRef<'a, 'd> {
    pub(super) fn negotiated(&self) -> Negotiated {
        self.port.with_shared(|shared| shared.negotiated)
    }

    pub(super) fn with_channel<R>(
        &self,
        channel: u16,
        f: impl for<'s> FnOnce(&'s mut ChannelState<'d>) -> R,
    ) -> Option<R> {
        self.port
            .with_shared(|shared| shared.with_channel_mut(channel, f))
    }

    pub(super) fn try_enqueue(&self, bytes: Outbound) -> Result<(), Outbound> {
        self.sender.try_enqueue(bytes)
    }
}

impl<'d> ConnectorRef<'d> {
    pub(super) fn session(&self) -> SessionRef<'d> {
        SessionRef { port: self.port }
    }

    pub(super) fn is_active(&self, target: Token) -> bool {
        self.port.io.is_active(target)
    }

    pub(super) fn with_state<R>(
        &self,
        target: Token,
        f: impl for<'s> FnOnce(StateRef<'s, 'd>) -> R,
    ) -> Option<R> {
        self.port.io.with_sender(target, |sender| {
            f(StateRef {
                sender,
                port: self.port,
            })
        })
    }

    pub(super) fn poll_target(
        &self,
        waker: Waker<'d>,
        missing: Missing,
    ) -> Poll<Result<Option<Token>, Error>> {
        let state = self.session().with_shared(|shared| {
            (
                shared.fatal.clone(),
                shared.auto_recovery_enabled(),
                shared.ready_conn,
            )
        });
        if let Some(error) = state.0 {
            return Poll::Ready(Err(error));
        }
        if let Some(target) = state.2
            && self.is_active(target)
        {
            return Poll::Ready(Ok(Some(target)));
        }
        let wait =
            matches!(missing, Missing::Wait) || matches!(missing, Missing::Recover) && state.1;
        let registered = self.session().with_shared(|shared| {
            shared.clear_dead_ready_conn();
            !wait || shared.try_register_handshake_waker(waker)
        });
        if !registered {
            Poll::Ready(Err(Error::WaiterCapacity))
        } else if wait {
            Poll::Pending
        } else {
            Poll::Ready(Ok(None))
        }
    }
}

impl<'a, 'd, S, E> AmqpHandle<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn connector(self) -> ConnectorRef<'d> {
        ConnectorRef { port: self.port }
    }

    pub(in crate::runtime) fn timer(self) -> &'d Timer<'d, 0> {
        self.port.timer()
    }

    pub(in crate::runtime) fn encode(
        self,
        f: impl FnOnce(&mut Outbound),
    ) -> Result<Outbound, Error> {
        self.port.encode(f)
    }
}
