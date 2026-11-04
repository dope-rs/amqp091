use std::mem;

use dope::core::driver::schedule::clock;
use dope::net::link::pool::change::opening;
use o3::mem::rc;

use crate::failure::{recovery, terminal, wire};
use crate::protocol::wire::text::resident;
use crate::state::conn::{Change, Start, status};
use crate::state::epoch;
use crate::{failure, protocol};

pub(crate) struct Core<'c, 'b, 'd, const ID: u8> {
    pub lifecycle: Lifecycle<'c, 'b, 'd, ID>,
    pub(crate) input: super::Input<'d, ID>,
    blocked: Option<resident::Exact<'d>>,
    negotiated: protocol::Negotiated,
}

pub(crate) struct Lifecycle<'c, 'b, 'd, const ID: u8> {
    status: status::Status<'c, 'b, 'd, ID>,
    pub(crate) previous: Option<failure::Exit<'c, 'b, 'd, ID>>,
    generation: u64,
    transport_deadline: Option<clock::Deadline<'d>>,
}

impl<'c, 'b, 'd, const ID: u8> Core<'c, 'b, 'd, ID> {
    pub(crate) fn new() -> Self {
        Self {
            lifecycle: Lifecycle::new(),
            input: super::Input::Empty,
            blocked: None,
            negotiated: protocol::Negotiated::default(),
        }
    }

    pub(crate) fn begin(
        &mut self,
        target: epoch::Id<'d, ID>,
        hold: terminal::Reservation<'c, 'b, 'd>,
    ) -> Start<'c, 'b, 'd, ID> {
        let start = self.lifecycle.begin(target, hold);
        match &start {
            Start::Ignored => {}
            Start::Applied | Start::Replaced { .. } => {
                self.input = super::Input::Empty;
                self.unblock();
            }
        }
        start
    }

    pub(crate) fn mark_open(&mut self, target: epoch::Id<'d, ID>) -> Result<(), failure::Error> {
        self.lifecycle.mark_open(target)?;
        self.unblock();
        Ok(())
    }

    pub(crate) fn start_close(&mut self, target: epoch::Id<'d, ID>) -> Result<(), failure::Error> {
        self.lifecycle.start_close(target)?;
        self.unblock();
        Ok(())
    }

    pub(crate) fn try_fail(
        &mut self,
        target: epoch::Id<'d, ID>,
        failure: failure::Error<resident::Short<'b, 'd>>,
    ) -> Result<terminal::Lease<'c, 'b, 'd>, failure::Error<resident::Short<'b, 'd>>> {
        let cause = self.lifecycle.try_fail(target, failure)?;
        self.unblock();
        Ok(cause)
    }

    pub(crate) fn finish_close(
        &mut self,
        target: epoch::Id<'d, ID>,
    ) -> Result<terminal::Lease<'c, 'b, 'd>, failure::Error> {
        let cause = self.lifecycle.finish_close(target)?;
        self.unblock();
        Ok(cause)
    }

    pub(crate) fn retire(
        &mut self,
        target: epoch::Id<'d, ID>,
        reason: failure::Reason,
    ) -> status::Retirement {
        let current = self.lifecycle.status.source() == Some(target);
        let retire = self.lifecycle.retire(target, reason);
        if current {
            self.unblock();
        }
        retire
    }

    pub(crate) fn disconnect<R>(
        &mut self,
        target: epoch::Id<'d, ID>,
        reason: failure::Reason,
        finish: impl FnOnce(&terminal::Lease<'c, 'b, 'd>) -> R,
    ) -> Option<R> {
        let next = self.lifecycle.disconnect(target, reason, finish);
        if next.is_some() {
            self.unblock();
        }
        next
    }

    pub(crate) fn fail<P: recovery::Policy, R>(
        &mut self,
        source: epoch::Id<'d, ID>,
        failure: recovery::Fault<'b, 'd, P>,
        reserve: impl FnOnce() -> Option<terminal::Reservation<'c, 'b, 'd>>,
        finish: impl FnOnce(&terminal::Lease<'c, 'b, 'd>, status::Retirement) -> R,
    ) -> Option<R> {
        let next = self.lifecycle.fail(source, failure, reserve, finish);
        if next.is_some() {
            self.unblock();
        }
        next
    }
}

impl<'c, 'b, 'd, const ID: u8> Lifecycle<'c, 'b, 'd, ID> {
    fn new() -> Self {
        Self {
            status: status::Status::AwaitingTransport,
            previous: None,
            generation: 0,
            transport_deadline: None,
        }
    }

    fn begin(
        &mut self,
        target: epoch::Id<'d, ID>,
        hold: terminal::Reservation<'c, 'b, 'd>,
    ) -> Start<'c, 'b, 'd, ID> {
        match mem::replace(&mut self.status, status::Status::Handshaking(target, hold)) {
            status::Status::AwaitingTransport | status::Status::AdmissionRejected => Start::Applied,
            status::Status::Reconnecting(epoch, cause) => Start::Replaced { epoch, cause },
            prior @ (status::Status::Handshaking(_, _)
            | status::Status::OpeningFailed(_)
            | status::Status::Open(_, _)
            | status::Status::Ready(_, _)
            | status::Status::Closing(_, _, _)
            | status::Status::Closed(_, _, _, _)
            | status::Status::Failed(_, _)) => {
                self.status = prior;
                Start::Ignored
            }
        }
    }

    pub(crate) fn bind(&mut self, expiry: Option<clock::Deadline<'d>>) {
        self.transport_deadline = expiry;
    }

    pub(crate) const fn status(&self) -> &status::Status<'c, 'b, 'd, ID> {
        &self.status
    }

    pub(crate) fn reject_cause(&mut self, cause: Option<opening::Exhaustion>) -> Change {
        if !matches!(
            self.status,
            status::Status::AwaitingTransport | status::Status::Reconnecting(_, _)
        ) {
            return Change::Ignored;
        }
        let next = cause.map_or(
            status::Status::AdmissionRejected,
            status::Status::OpeningFailed,
        );
        if let status::Status::Reconnecting(_, terminal::State::Retired(exit)) =
            mem::replace(&mut self.status, next)
        {
            self.previous = Some(exit);
        }
        Change::Applied
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn deadline(&self) -> Option<clock::Deadline<'d>> {
        self.status.epoch()?;
        self.transport_deadline
    }
}

impl<'c, 'b, 'd, const ID: u8> Core<'c, 'b, 'd, ID> {
    pub(crate) fn negotiated(&self) -> protocol::Negotiated {
        self.negotiated
    }

    pub(crate) fn try_frame<Text>(&self, size: usize) -> Result<(), failure::Error<Text>> {
        let max = self.negotiated.frame_max;
        if max != 0 && size > max as usize {
            return Err(failure::Error::FrameTooLarge {
                size: size as u64,
                max,
            });
        }
        Ok(())
    }

    pub(crate) fn set_tune(&mut self, tune: protocol::Negotiated) {
        self.negotiated = tune;
    }

    pub(crate) fn block(&mut self, reason: resident::Exact<'d>) {
        self.blocked = Some(reason);
    }

    pub(crate) fn unblock(&mut self) {
        self.blocked = None;
    }

    pub(crate) fn blocked(&self) -> Option<&str> {
        self.blocked.as_ref().map(resident::Exact::as_str)
    }

    pub(crate) fn try_cause(&self) -> Result<Option<resident::Exact<'d>>, rc::Error> {
        self.blocked
            .as_ref()
            .map(resident::Exact::try_clone)
            .transpose()
    }
}

impl<'c, 'b, 'd, const ID: u8> Lifecycle<'c, 'b, 'd, ID> {
    fn mark_open(&mut self, token: epoch::Id<'d, ID>) -> Result<(), failure::Error> {
        let status = mem::replace(&mut self.status, status::Status::AwaitingTransport);
        let (current, hold) = match status {
            status::Status::Handshaking(current, hold) if current == token => (current, hold),
            status => {
                self.status = status;
                return Err(failure::Error::Closed);
            }
        };
        let Some(next) = self.generation.checked_add(1) else {
            self.status = status::Status::Handshaking(current, hold);
            return Err(failure::Error::Capacity(wire::Capacity::ChannelGeneration));
        };
        self.generation = next;
        self.status = status::Status::Open(current, hold);
        Ok(())
    }

    pub(crate) fn mark_ready(&mut self, token: epoch::Id<'d, ID>) -> Result<(), failure::Error> {
        let hold = match mem::replace(&mut self.status, status::Status::AwaitingTransport) {
            status::Status::Open(current, hold) if current == token => hold,
            status => {
                self.status = status;
                return Err(failure::Error::Closed);
            }
        };
        self.status = status::Status::Ready(token, hold);
        Ok(())
    }

    fn start_close(&mut self, target: epoch::Id<'d, ID>) -> Result<(), failure::Error> {
        let hold = match mem::replace(&mut self.status, status::Status::AwaitingTransport) {
            status::Status::Open(current, hold) | status::Status::Ready(current, hold)
                if current == target =>
            {
                hold
            }
            status => {
                self.status = status;
                return Err(failure::Error::Closed);
            }
        };
        let cause = hold.commit(self.generation, failure::Error::Closed);
        self.status = status::Status::Closing(target, cause, status::CloseKind::Graceful);
        Ok(())
    }

    fn try_fail(
        &mut self,
        target: epoch::Id<'d, ID>,
        failure: failure::Error<resident::Short<'b, 'd>>,
    ) -> Result<terminal::Lease<'c, 'b, 'd>, failure::Error<resident::Short<'b, 'd>>> {
        let hold = match mem::replace(&mut self.status, status::Status::AwaitingTransport) {
            status::Status::Open(current, hold) if current == target => hold,
            status::Status::Ready(current, hold) if current == target => hold,
            status::Status::Handshaking(current, hold) if current == target => hold,
            status => {
                self.status = status;
                return Err(failure);
            }
        };
        let (stored, cause) = hold.commit_pair(self.generation, failure);
        self.status = status::Status::Closing(target, stored, status::CloseKind::Failure);
        Ok(cause)
    }

    fn finish_close(
        &mut self,
        target: epoch::Id<'d, ID>,
    ) -> Result<terminal::Lease<'c, 'b, 'd>, failure::Error> {
        let (cause, kind) = match mem::replace(&mut self.status, status::Status::AwaitingTransport)
        {
            status::Status::Closing(current, cause, kind) if current == target => (cause, kind),
            status => {
                self.status = status;
                return Err(failure::Error::Protocol(wire::Fault::CloseClosed));
            }
        };
        let returned = match cause.try_clone() {
            Ok(returned) => returned,
            Err(error) => {
                self.status = status::Status::Closing(target, cause, kind);
                return Err(failure::Error::Retain(error));
            }
        };
        self.status = status::Status::Closed(
            target,
            terminal::State::Active(cause),
            kind,
            status::CloseOutcome::Graceful,
        );
        Ok(returned)
    }

    fn retire(&mut self, target: epoch::Id<'d, ID>, reason: failure::Reason) -> status::Retirement {
        let status = mem::replace(&mut self.status, status::Status::AwaitingTransport);
        match status {
            status::Status::Handshaking(current, hold)
            | status::Status::Open(current, hold)
            | status::Status::Ready(current, hold)
                if current == target =>
            {
                let class = recovery::Session::classify_reason(&reason);
                let cause = hold.commit(self.generation, failure::Error::Disconnected(reason));
                let cause = terminal::State::Active(cause).retire(reason);
                self.previous = None;
                match class {
                    recovery::Class::Recoverable => {
                        self.status = status::Status::Reconnecting(target, cause);
                        status::Retirement::Reconnect
                    }
                    recovery::Class::Fatal => {
                        self.status = status::Status::Failed(target, cause);
                        status::Retirement::Permanent
                    }
                }
            }
            status::Status::Reconnecting(current, cause) if current == target => {
                self.status = status::Status::Reconnecting(current, cause.retire(reason));
                self.previous = None;
                status::Retirement::Reconnect
            }
            status::Status::Closing(current, cause, kind) if current == target => {
                self.status = status::Status::Closed(
                    current,
                    terminal::State::Active(cause).retire(reason),
                    kind,
                    status::CloseOutcome::HandshakeInterrupted,
                );
                self.previous = None;
                status::Retirement::Permanent
            }
            status::Status::Closed(current, cause, kind, outcome) if current == target => {
                self.status = status::Status::Closed(current, cause.retire(reason), kind, outcome);
                self.previous = None;
                status::Retirement::Permanent
            }
            status::Status::Failed(current, cause) if current == target => {
                self.status = status::Status::Failed(current, cause.retire(reason));
                self.previous = None;
                status::Retirement::Permanent
            }
            status => {
                self.status = status;
                status::Retirement::Permanent
            }
        }
    }

    fn disconnect<R>(
        &mut self,
        target: epoch::Id<'d, ID>,
        reason: failure::Reason,
        finish: impl FnOnce(&terminal::Lease<'c, 'b, 'd>) -> R,
    ) -> Option<R> {
        if self.status.source() != Some(target) {
            return None;
        }
        self.retire(target, reason);
        let cause = self.status.terminal(target)?;
        Some(finish(cause))
    }

    fn fail<P: recovery::Policy, R>(
        &mut self,
        source: epoch::Id<'d, ID>,
        failure: recovery::Fault<'b, 'd, P>,
        reserve: impl FnOnce() -> Option<terminal::Reservation<'c, 'b, 'd>>,
        finish: impl FnOnce(&terminal::Lease<'c, 'b, 'd>, status::Retirement) -> R,
    ) -> Option<R> {
        if !self.status.can_accept(source, status::Input::Failure) {
            return None;
        }
        let (class, error) = P::classify(failure);
        let status = mem::replace(&mut self.status, status::Status::AwaitingTransport);
        let (output, next) = match status {
            status::Status::Handshaking(_, hold)
            | status::Status::Open(_, hold)
            | status::Status::Ready(_, hold) => {
                let cause = hold.commit(self.generation, error);
                match class {
                    recovery::Class::Recoverable => (
                        finish(&cause, status::Retirement::Reconnect),
                        status::Status::Reconnecting(source, terminal::State::Active(cause)),
                    ),
                    recovery::Class::Fatal => (
                        finish(&cause, status::Retirement::Permanent),
                        status::Status::Failed(source, terminal::State::Active(cause)),
                    ),
                }
            }
            status::Status::Reconnecting(current, cause) => match class {
                recovery::Class::Recoverable => (
                    finish(&cause, status::Retirement::Reconnect),
                    status::Status::Reconnecting(current, cause),
                ),
                recovery::Class::Fatal => {
                    let cause = match reserve() {
                        Some(hold) => {
                            if let terminal::State::Retired(exit) = cause {
                                self.previous = Some(exit);
                            }
                            terminal::State::Active(hold.commit(self.generation, error))
                        }
                        None => cause,
                    };
                    (
                        finish(&cause, status::Retirement::Permanent),
                        status::Status::Failed(current, cause),
                    )
                }
            },
            status::Status::Closing(current, cause, kind) => (
                finish(&cause, status::Retirement::Permanent),
                status::Status::Closed(
                    current,
                    terminal::State::Active(cause),
                    kind,
                    status::CloseOutcome::HandshakeInterrupted,
                ),
            ),
            status => {
                self.status = status;
                return None;
            }
        };
        self.status = next;
        Some(output)
    }
}

const _: () = assert!(
    mem::size_of::<Option<clock::Deadline<'static>>>()
        == mem::size_of::<Option<clock::Moment<'static>>>()
);
