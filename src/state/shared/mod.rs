use std::mem;

use dope::core::driver::schedule::clock;
use dope::core::driver::{self};
use dope::net::link::pool::change::opening;

use crate::failure;
use crate::failure::{recovery, terminal};
use crate::state::channel;
use crate::state::conn::core::Core;
use crate::state::conn::recovery::Recovery;
use crate::state::conn::status::Retirement;
use crate::state::{channels, conn, epoch};

mod root;
pub(crate) mod routes;

pub(crate) use root::Root;

#[must_use = "connection start can release a route owned by the previous transport"]
pub(crate) enum Start<'d, const ID: u8> {
    Ignored,
    Applied,
    Route { epoch: epoch::Id<'d, ID> },
}

#[must_use = "connection change can release a blocked content route"]
pub(crate) enum Disconnect {
    Ignored,
    Applied { freed: channel::Freed },
}

#[must_use = "connection failure selects the transport retirement"]
pub(crate) enum Failure {
    Ignored,
    Applied(Retirement),
}

pub(crate) struct Session<'r, 'c, 'b, 'd, const ID: u8> {
    pub connection: Core<'c, 'b, 'd, ID>,
    pub recovery: Recovery<'c, 'd, ID>,
    pub routes: routes::Routes<'r, 'c, 'b, 'd, ID>,
}

pub(crate) struct Edit<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    pub connection: &'a mut Core<'c, 'b, 'd, ID>,
    pub recovery: &'a mut Recovery<'c, 'd, ID>,
    pub routes: routes::Edit<'a, 'r, 'c, 'b, 'd, ID>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Session<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn try_new(
        plan: channels::Plan,
        driver: driver::Reference<'d>,
        permits: &crate::permits::Registry<'c, 'd>,
    ) -> Result<Self, channels::Error> {
        Ok(Self {
            connection: Core::new(),
            recovery: Recovery::new(),
            routes: routes::Routes::try_new(plan, driver, permits)?,
        })
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Edit<'_, 'r, 'c, 'b, 'd, ID> {
    pub(crate) fn begin(
        &mut self,
        target: epoch::Id<'d, ID>,
        cause: terminal::Reservation<'c, 'b, 'd>,
        until: Option<clock::Deadline<'d>>,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> Start<'d, ID> {
        match self.connection.begin(target, cause) {
            conn::Start::Ignored => Start::Ignored,
            conn::Start::Applied => {
                self.connection.lifecycle.bind(until);
                Start::Applied
            }
            conn::Start::Replaced { epoch, cause } => {
                self.connection.lifecycle.bind(until);
                let freed = finish(self.recovery, &mut self.routes, &cause, stores);
                if let terminal::State::Retired(exit) = cause {
                    self.connection.lifecycle.previous = Some(exit);
                }
                match freed {
                    channel::Freed::Available => Start::Applied,
                    channel::Freed::Route => Start::Route { epoch },
                }
            }
        }
    }

    pub(crate) fn disconnect(
        &mut self,
        target: epoch::Id<'d, ID>,
        reason: failure::Reason,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> Disconnect {
        let Some(freed) = self.connection.disconnect(target, reason, |cause| {
            finish(self.recovery, &mut self.routes, cause, stores)
        }) else {
            return Disconnect::Ignored;
        };
        Disconnect::Applied { freed }
    }

    pub(crate) fn reject(&mut self, cause: Option<opening::Exhaustion>) -> conn::Change {
        self.connection.lifecycle.reject_cause(cause)
    }

    pub(crate) fn mark_open(&mut self, token: epoch::Id<'d, ID>) -> Result<(), failure::Error> {
        self.connection.mark_open(token)?;
        if self.connection.lifecycle.generation() == 1 || self.routes.next_tracked(None).is_none() {
            self.connection.lifecycle.mark_ready(token)?;
        } else {
            self.recovery.start(token);
        }
        Ok(())
    }

    pub(crate) fn fail<P: recovery::Policy>(
        &mut self,
        source: epoch::Id<'d, ID>,
        error: recovery::Fault<'b, 'd, P>,
        take: impl FnOnce() -> Option<terminal::Reservation<'c, 'b, 'd>>,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> Failure {
        let Some(retire) =
            self.connection.fail(source, error, take, |cause, retire| {
                match finish(self.recovery, &mut self.routes, cause, stores) {
                    channel::Freed::Available | channel::Freed::Route => retire,
                }
            })
        else {
            return Failure::Ignored;
        };
        Failure::Applied(retire)
    }

    pub(crate) fn retire(
        &mut self,
        target: epoch::Id<'d, ID>,
        reason: failure::Reason,
    ) -> Retirement {
        let live = self.connection.lifecycle.status().source() == Some(target);
        let state = self.connection.retire(target, reason);
        if live {
            self.recovery.reset();
        }
        state
    }

    pub(crate) fn retire_epoch(&mut self, target: epoch::Id<'d, ID>) -> conn::Change {
        if self.connection.lifecycle.status().epoch() != Some(target) {
            return conn::Change::Ignored;
        }
        self.connection.retire(target, failure::Reason::Transport);
        self.recovery.reset();
        conn::Change::Applied
    }
}

fn finish<'r, 'c, 'b, 'd, const ID: u8>(
    recovery: &mut Recovery<'c, 'd, ID>,
    routes: &mut routes::Edit<'_, 'r, 'c, 'b, 'd, ID>,
    cause: &terminal::Lease<'c, 'b, 'd>,
    stores: channel::Stores<'r, 'c, 'b, 'd>,
) -> channel::Freed {
    recovery.reset();
    let freed = routes.transport_ended(cause, stores);
    routes.sweep();
    freed
}

const _: () = {
    assert!(mem::size_of::<Start<'static, 0>>() == 3 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<conn::Start<'static, 'static, 'static, 0>>()
            == mem::size_of::<epoch::Id<'static, 0>>()
                + mem::size_of::<terminal::State<'static, 'static, 'static, 0>>()
    );
    assert!(mem::size_of::<Disconnect>() == mem::size_of::<conn::Change>());
    assert!(mem::size_of::<Failure>() == 1);
};
