use dope::manifold::app::receive;
use dope::manifold::client::{close, deadline};
use dope::net::link::egress::reserve::exact;
use dope::net::link::event;

/// Transport-level reason associated with a retired AMQP connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    Local(close::LocalOrigin),
    Capacity(close::Capacity),
    EndpointRetired,
    Transport,
    ConnectTimeout,
    InboundTimeout,
    SendTimeout,
    LifetimeTimeout,
    Protocol,
    Remote,
    Receive(event::ParkError),
    Ingress(receive::Failure),
    Egress(exact::Failure),
}

impl From<close::Reason> for Reason {
    fn from(reason: close::Reason) -> Self {
        match reason {
            close::Reason::Local(origin) => Self::Local(origin),
            close::Reason::Capacity(cause) => Self::Capacity(cause),
            close::Reason::EndpointRetired => Self::EndpointRetired,
            close::Reason::Transport => Self::Transport,
            close::Reason::Timeout(deadline::Kind::Connect) => Self::ConnectTimeout,
            close::Reason::Timeout(deadline::Kind::Inbound) => Self::InboundTimeout,
            close::Reason::Timeout(deadline::Kind::Send) => Self::SendTimeout,
            close::Reason::Timeout(deadline::Kind::Lifetime) => Self::LifetimeTimeout,
            close::Reason::Protocol => Self::Protocol,
            close::Reason::Remote => Self::Remote,
            close::Reason::Receive(error) => Self::Receive(error),
            close::Reason::Ingress(cause) => Self::Ingress(cause),
            close::Reason::Egress(cause) => Self::Egress(cause),
        }
    }
}

const _: () = assert!(core::mem::size_of::<Reason>() == core::mem::size_of::<close::Reason>());
