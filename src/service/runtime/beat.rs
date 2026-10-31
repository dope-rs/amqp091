use std::{cell, num, time};

use dope::core::driver::schedule::{self, app, clock};
use o3::base::cell::{local, region};

use crate::failure;
use crate::protocol::wire::frame;
use crate::service::{self, port};
use crate::state::epoch;

pub(in crate::service) struct Heartbeat<'d, const ID: u8> {
    deadline: cell::Cell<Option<Deadline<'d, ID>>>,
}

#[derive(Clone, Copy)]
struct Deadline<'d, const ID: u8> {
    target: epoch::Id<'d, ID>,
    at: clock::Deadline<'d>,
}

impl<'d, const ID: u8> Heartbeat<'d, ID> {
    pub(in crate::service) const fn new() -> Self {
        Self {
            deadline: cell::Cell::new(None),
        }
    }

    fn beat(
        port: &port::Domain<'_, '_, '_, 'd, ID>,
        region: &region::Token<region::Identity<'d>>,
    ) -> Option<(epoch::Id<'d, ID>, num::NonZero<u16>)> {
        port.session.read(local::Read::new(region), |shared| {
            let target = shared.connection.lifecycle.status().epoch()?;
            let secs = num::NonZero::new(shared.connection.negotiated().heartbeat)?;
            Some((target, secs))
        })
    }

    pub(in crate::service) fn emit(
        &self,
        port: &port::Domain<'_, '_, '_, 'd, ID>,
        work: app::Work<'_, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let Some((target, secs)) = Self::beat(port, region) else {
            self.deadline.set(None);
            return;
        };
        let period = time::Duration::from_millis(u64::from(secs.get()) * 500);
        let now = work.clock().now();
        let next = now.clamp_add(period);
        let Some(due) = self.deadline.get() else {
            self.deadline.set(Some(Deadline { target, at: next }));
            return;
        };
        if due.target != target {
            self.deadline.set(Some(Deadline { target, at: next }));
            return;
        }
        if due.at > now {
            return;
        }
        let Some(turn) = work.permit() else {
            return;
        };
        turn.run(|| {
            self.deadline.set(Some(Deadline { target, at: next }));
            if let service::Admit::Error(error) = service::Admission::new(port).admit(
                local::Access::new(region),
                target,
                None,
                |output| frame::Encoder::new(output).heartbeat(),
                |shared| {
                    (shared.connection.lifecycle.status().epoch() == Some(target))
                        .then_some(())
                        .ok_or(failure::Error::Closed)
                },
            ) {
                port.fail(local::Access::new(region), target, error);
            }
        });
    }

    pub(in crate::service) fn progress(
        &self,
        port: &port::Domain<'_, '_, '_, 'd, ID>,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        let Some((target, _)) = Self::beat(port, region) else {
            self.deadline.set(None);
            return schedule::Progress::Quiescent;
        };
        match self.deadline.get() {
            Some(due) if due.target == target => schedule::Progress::until(due.at),
            _ => schedule::Progress::Runnable,
        }
    }
}
