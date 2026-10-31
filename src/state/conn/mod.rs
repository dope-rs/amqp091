use crate::failure::terminal;
use crate::protocol;
use crate::state::epoch;

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

#[derive(Default)]
pub struct Transport {
    pub(crate) phase: phase::Phase,
    pub negotiated: protocol::Negotiated,
}

impl Transport {
    pub fn is_ready(&self) -> bool {
        self.phase == phase::Phase::Ready
    }
}
