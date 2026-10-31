use std::mem;

use dope::manifold::client::close;
use dope::net::link::pool::change::opening;

use crate::failure::{self, terminal, wire};
use crate::state::epoch;

#[derive(Clone, Copy)]
pub(crate) enum Input {
    Frame,
    Failure,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retirement {
    Reconnect,
    Permanent,
}

impl Retirement {
    pub(crate) const fn policy(self) -> close::Policy {
        match self {
            Self::Reconnect => close::Policy::Reconnect,
            Self::Permanent => close::Policy::Permanent,
        }
    }

    pub(crate) fn abort(self, reason: close::Reason) -> close::Directive {
        match self {
            Self::Reconnect => close::Directive::cut_retry(reason),
            Self::Permanent => close::Directive::cut_final(reason),
        }
    }

    pub(crate) fn drain(self, reason: close::Reason) -> close::Directive {
        match self {
            Self::Reconnect => close::Directive::reconnect(reason),
            Self::Permanent => close::Directive::permanent(reason),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseOutcome {
    Graceful,
    HandshakeInterrupted,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseKind {
    Graceful,
    Failure,
}

pub(crate) enum CloseResult<'a, 'c, 'b, 'd, const ID: u8> {
    Pending,
    Complete,
    Terminal(&'a terminal::Lease<'c, 'b, 'd>),
    Unknown,
}

pub(crate) enum Status<'c, 'b, 'd, const ID: u8> {
    AwaitingTransport,
    AdmissionRejected,
    OpeningFailed(opening::Exhaustion),
    Handshaking(epoch::Id<'d, ID>, terminal::Reservation<'c, 'b, 'd>),
    Open(epoch::Id<'d, ID>, terminal::Reservation<'c, 'b, 'd>),
    Ready(epoch::Id<'d, ID>, terminal::Reservation<'c, 'b, 'd>),
    Reconnecting(epoch::Id<'d, ID>, terminal::State<'c, 'b, 'd, ID>),
    Closing(epoch::Id<'d, ID>, terminal::Lease<'c, 'b, 'd>, CloseKind),
    Closed(
        epoch::Id<'d, ID>,
        terminal::State<'c, 'b, 'd, ID>,
        CloseKind,
        CloseOutcome,
    ),
    Failed(epoch::Id<'d, ID>, terminal::State<'c, 'b, 'd, ID>),
}

impl<'c, 'b, 'd, const ID: u8> Status<'c, 'b, 'd, ID> {
    pub(crate) fn source(&self) -> Option<epoch::Id<'d, ID>> {
        match self {
            Self::Handshaking(source, _)
            | Self::Open(source, _)
            | Self::Ready(source, _)
            | Self::Reconnecting(source, _)
            | Self::Closing(source, _, _)
            | Self::Closed(source, _, _, _)
            | Self::Failed(source, _) => Some(*source),
            Self::AwaitingTransport | Self::AdmissionRejected | Self::OpeningFailed(_) => None,
        }
    }

    pub(crate) fn ready(&self) -> Option<epoch::Id<'d, ID>> {
        match self {
            Self::Ready(target, _) => Some(*target),
            _ => None,
        }
    }

    pub(crate) fn epoch(&self) -> Option<epoch::Id<'d, ID>> {
        match self {
            Self::Open(target, _) | Self::Ready(target, _) => Some(*target),
            _ => None,
        }
    }

    pub(crate) fn rejection(&self) -> Option<failure::Error> {
        match self {
            Self::OpeningFailed(cause) => Some(failure::Error::Opening(*cause)),
            Self::AdmissionRejected => {
                Some(failure::Error::Capacity(wire::Capacity::TerminalCause))
            }
            _ => None,
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        matches!(
            self,
            Self::AdmissionRejected
                | Self::OpeningFailed(_)
                | Self::Closing(_, _, CloseKind::Failure)
                | Self::Closed(_, _, CloseKind::Failure, _)
                | Self::Failed(_, _)
        )
    }

    pub(crate) fn failure(&self) -> Option<&terminal::Lease<'c, 'b, 'd>> {
        match self {
            Self::Closing(_, cause, CloseKind::Failure) => Some(cause),
            Self::Closed(_, cause, CloseKind::Failure, _) | Self::Failed(_, cause) => Some(cause),
            _ => None,
        }
    }

    pub(crate) fn terminal(
        &self,
        target: epoch::Id<'d, ID>,
    ) -> Option<&terminal::Lease<'c, 'b, 'd>> {
        match self {
            Self::Closing(current, cause, _) if *current == target => Some(cause),
            Self::Reconnecting(current, cause)
            | Self::Closed(current, cause, _, _)
            | Self::Failed(current, cause)
                if *current == target =>
            {
                Some(cause)
            }
            _ => None,
        }
    }

    pub(crate) fn exit(&self) -> Option<&failure::Exit<'c, 'b, 'd, ID>> {
        match self {
            Self::Reconnecting(_, cause)
            | Self::Closed(_, cause, _, _)
            | Self::Failed(_, cause) => cause.exit(),
            _ => None,
        }
    }

    pub(crate) fn is_closing(&self) -> bool {
        matches!(self, Self::Closing(_, _, _))
    }

    pub(crate) fn can_accept(&self, source: epoch::Id<'d, ID>, input: Input) -> bool {
        let current = match (self, input) {
            (
                Self::Handshaking(current, _)
                | Self::Open(current, _)
                | Self::Ready(current, _)
                | Self::Closing(current, _, _),
                _,
            )
            | (Self::Reconnecting(current, _), Input::Failure) => current,
            _ => return false,
        };
        *current == source
    }

    pub(crate) fn close_result(
        &self,
        target: epoch::Id<'d, ID>,
    ) -> CloseResult<'_, 'c, 'b, 'd, ID> {
        match self {
            Self::Closing(current, _, CloseKind::Graceful) if *current == target => {
                CloseResult::Pending
            }
            Self::Closed(current, _, CloseKind::Graceful, CloseOutcome::Graceful)
                if *current == target =>
            {
                CloseResult::Complete
            }
            Self::Closing(current, cause, _) if *current == target => CloseResult::Terminal(cause),
            Self::Reconnecting(current, cause)
            | Self::Closed(current, cause, _, _)
            | Self::Failed(current, cause)
                if *current == target =>
            {
                CloseResult::Terminal(cause)
            }
            _ => CloseResult::Unknown,
        }
    }

    pub(crate) fn retirement(&self, target: epoch::Id<'d, ID>) -> Retirement {
        match self {
            Self::Handshaking(current, _)
            | Self::Open(current, _)
            | Self::Ready(current, _)
            | Self::Reconnecting(current, _)
                if *current == target =>
            {
                Retirement::Reconnect
            }
            Self::AwaitingTransport
            | Self::AdmissionRejected
            | Self::OpeningFailed(_)
            | Self::Handshaking(_, _)
            | Self::Open(_, _)
            | Self::Ready(_, _)
            | Self::Reconnecting(_, _)
            | Self::Closing(_, _, _)
            | Self::Closed(_, _, _, _)
            | Self::Failed(_, _) => Retirement::Permanent,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Retirement>() == 1);
    assert!(
        mem::size_of::<CloseResult<'static, 'static, 'static, 'static, 0>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<CloseResult<'static, 'static, 'static, 'static, 0>>()
            == mem::size_of::<CloseResult<'static, 'static, 'static, 'static, 255>>()
    );
};
