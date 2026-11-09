use crate::failure::terminal;
use crate::protocol::{self, method};
use crate::state::channel::methods::MethodKey;
use crate::state::{channels, epoch};

pub(crate) mod core;
pub(crate) mod phase;
pub(crate) mod recovery;
pub(crate) mod status;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Change {
    Applied,
    Ignored,
}

pub(crate) enum Start<'c, 'b, 'd, const ID: u8> {
    Ignored,
    Applied,
    Replaced {
        epoch: epoch::Id<'d, ID>,
        cause: terminal::State<'c, 'b, 'd, ID>,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum Owner<'d, const ID: u8> {
    Connection {
        phase: phase::Phase,
        closing: bool,
    },
    Unknown,
    Channel {
        lease: channels::Lease<'d, ID>,
        sent: Option<(MethodKey, method::Kind)>,
        published: u64,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum Input<'d, const ID: u8> {
    Empty,
    Unbound(Option<channels::Generation>, phase::Phase),
    Held(Owner<'d, ID>),
    Partial(Owner<'d, ID>),
}

impl<'d, const ID: u8> Input<'d, ID> {
    pub(crate) const fn is_ready(self) -> bool {
        matches!(self, Self::Empty | Self::Partial(_))
    }

    pub(crate) const fn owner(self) -> Option<Owner<'d, ID>> {
        match self {
            Self::Held(owner) | Self::Partial(owner) => Some(owner),
            Self::Empty | Self::Unbound(..) => None,
        }
    }

    pub(crate) const fn lease(self) -> Option<channels::Lease<'d, ID>> {
        match self.owner() {
            Some(Owner::Channel { lease, .. }) => Some(lease),
            _ => None,
        }
    }

    pub(crate) fn settle(
        &mut self,
        input: dope::manifold::client::session::Input,
        generation: Option<channels::Generation>,
        phase: phase::Phase,
    ) -> bool {
        use dope::manifold::client::session::Input;

        let ready = self.is_ready();
        *self = match (input, *self) {
            (Input::Empty, _) => Self::Empty,
            (Input::Partial, Self::Held(owner)) => Self::Partial(owner),
            (Input::Partial, owner) => owner,
            (Input::Yield | Input::Blocked, Self::Empty) => Self::Unbound(generation, phase),
            (Input::Yield | Input::Blocked, Self::Partial(owner)) => Self::Held(owner),
            (Input::Yield | Input::Blocked, owner) => owner,
        };
        !ready && self.is_ready()
    }
}

#[derive(Default)]
pub struct Transport {
    pub(crate) phase: phase::Phase,
    pub(crate) received: phase::Phase,
    pub negotiated: protocol::Negotiated,
}

impl Transport {
    pub fn is_ready(&self) -> bool {
        self.phase == phase::Phase::Ready
    }
}
