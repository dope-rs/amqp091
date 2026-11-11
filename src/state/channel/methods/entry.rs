use dope::core::driver::schedule::ready::wake::store;
use o3::depot::queue::indexed::keyed;
use o3::depot::queue::links;

use crate::failure::terminal;
use crate::failure::wire::turn;
use crate::protocol::wire::text::resident;
use crate::protocol::{Response, method};
use crate::state::channel::consumer;
use crate::state::channel::methods::{MethodKey, Plane};
use crate::state::layout;

pub(in crate::state::channel) enum Outbound<'b, 'd> {
    None,
    Consumer(consumer::Key),
    Orphan(resident::Short<'b, 'd>),
    Close,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    User,
    Detached,
    Replay,
}

pub(super) struct Pending<'b, 'd> {
    pub(super) expect: method::Kind,
    pub(in crate::state::channel::methods) outbound: Outbound<'b, 'd>,
    pub(super) waker: store::Entry<'d>,
    pub(super) origin: Origin,
    pub(super) sent: bool,
}

pub(super) struct Ready<'c, 'b, 'd> {
    pub(super) expect: method::Kind,
    pub(super) outcome: Result<Response<'b, 'd, consumer::Key>, terminal::Outcome<'c, 'b, 'd>>,
}

#[repr(u8)]
pub(super) enum Stage<'c, 'b, 'd> {
    Pending(Pending<'b, 'd>),
    Ready(Ready<'c, 'b, 'd>),
}

pub(crate) struct Entry<'c, 'b, 'd> {
    pub(super) link: keyed::Link<MethodKey, Plane>,
    pub(super) state: Stage<'c, 'b, 'd>,
}

impl<'b, 'd> Pending<'b, 'd> {
    fn new(expect: method::Kind, outbound: Outbound<'b, 'd>, origin: Origin) -> Self {
        Self {
            expect,
            outbound,
            waker: store::Entry::empty(),
            origin,
            sent: false,
        }
    }

    pub(super) fn user(expect: method::Kind) -> Self {
        Self::new(expect, Outbound::None, Origin::User)
    }

    pub(super) fn replay(expect: method::Kind) -> Self {
        Self::new(expect, Outbound::None, Origin::Replay)
    }

    pub(super) fn consumer(key: consumer::Key) -> Self {
        Self::new(
            method::Kind::CancelOk,
            Outbound::Consumer(key),
            Origin::Detached,
        )
    }

    pub(super) fn orphan(tag: resident::Short<'b, 'd>, origin: Origin) -> Self {
        Self::new(method::Kind::CancelOk, Outbound::Orphan(tag), origin)
    }

    pub(super) fn close() -> Self {
        Self::new(method::Kind::CloseOk, Outbound::Close, Origin::Detached)
    }

    pub(super) fn is_control(&self) -> bool {
        !matches!(self.outbound, Outbound::None)
    }

    pub(super) fn retiree(&self) -> Option<consumer::Key> {
        match &self.outbound {
            Outbound::Consumer(key) => Some(*key),
            Outbound::None | Outbound::Orphan(_) | Outbound::Close => None,
        }
    }

    pub(super) fn control(&self) -> turn::Control {
        match &self.outbound {
            Outbound::None => turn::Control::None,
            Outbound::Consumer(_) => turn::Control::Consumer,
            Outbound::Orphan(_) => turn::Control::Orphan,
            Outbound::Close => turn::Control::Close,
        }
    }

    pub(super) fn send(&mut self) -> Result<&mut Self, turn::Method> {
        if self.sent {
            return Err(turn::Method::Sent);
        }
        Ok(self)
    }
}

impl<'c, 'b, 'd> Entry<'c, 'b, 'd> {
    pub(super) fn new(index: links::Index, pending: Pending<'b, 'd>) -> Self {
        Self {
            link: keyed::Link::from(index),
            state: Stage::Pending(pending),
        }
    }

    pub(super) fn waiting(&mut self) -> Result<&mut Pending<'b, 'd>, turn::Method> {
        match &mut self.state {
            Stage::Pending(pending) if pending.sent => Err(turn::Method::Sent),
            Stage::Pending(pending) if pending.is_control() => Err(turn::Method::Control),
            Stage::Pending(pending) => Ok(pending),
            Stage::Ready(_) => Err(turn::Method::Ready),
        }
    }

    pub(super) fn get_mut(&mut self) -> Result<&mut Pending<'b, 'd>, turn::Method> {
        match &mut self.state {
            Stage::Pending(pending) => Ok(pending),
            Stage::Ready(_) => Err(turn::Method::Ready),
        }
    }
}

const _: () = layout::Layout::u64_words(8).prove::<Entry<'_, '_, '_>>();
