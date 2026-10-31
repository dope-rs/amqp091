use std::{mem, num};

use dope::core::driver::route::KeyTag;
use dope::core::driver::settings::{claim, errors};
use dope::core::driver::{settings, storage};
use dope::manifold::client::session::turns;
use o3::alloc;
use o3::base::time::Positive;
use o3::buffer::pool;
use o3::depot::capacity;
use o3::depot::queue::resident;

use crate::config;
use crate::state::{channels, conn};

pub mod app;
pub(crate) mod cancel;
pub(crate) mod content;
mod logic;
mod method;
mod plan;
mod port;
mod runtime;

pub use logic::Logic;
pub use port::Port;
pub(crate) use port::{Admission, Admit, Coord, Domain, EpochAccess, Waiters};

#[derive(Clone, Copy, Debug)]
pub enum Build {
    Allocation(alloc::Error),
    Compact(alloc::Exhausted),
    Control(alloc::Exhausted),
    Regular(alloc::Exhausted),
    Resident(storage::Exhausted),
    WaitEntries(alloc::Error),
    WakeCapacity(capacity::Error),
    WakeExhausted(alloc::Exhausted),
    WakeOverflow(alloc::Overflow),
    WakeLane(resident::Bounds<num::NonZero<u32>>),
    WakeIndex(num::TryFromIntError),
}

const _: () = assert!(mem::size_of::<Build>() == 5 * mem::size_of::<usize>());

impl From<resident::Error> for Build {
    fn from(error: resident::Error) -> Self {
        match error {
            resident::Error::Capacity(error) => Self::WakeCapacity(error),
            resident::Error::Exhausted(error) => Self::WakeExhausted(error),
            resident::Error::Overflow(error) => Self::WakeOverflow(error),
        }
    }
}

impl From<channels::Error> for Build {
    fn from(error: channels::Error) -> Self {
        match error {
            channels::Error::Allocation(error) => Self::Allocation(error),
            channels::Error::Lane(error) => Self::WakeLane(error),
            channels::Error::Index(error) => Self::WakeIndex(error),
        }
    }
}

/// Validated AMQP storage and connector plan.
pub struct Spec<const ID: u8 = 0> {
    config: config::Config,
    plan: plan::Plan,
    max_age: Option<Positive>,
}

impl<const ID: u8> settings::Resource for Spec<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(
            claim::Claim::new(settings::Routes::EMPTY).domains(settings::Domain::fixed::<2>()),
        )
    }
}

impl<const ID: u8> Spec<ID> {
    pub fn try_new(
        config: config::Config,
        capacity: usize,
        waits: usize,
    ) -> Result<Self, crate::failure::Error> {
        let plan = plan::Plan::try_new(&config, capacity, waits)?;
        Ok(Self {
            config,
            plan,
            max_age: None,
        })
    }

    #[must_use]
    pub const fn with_age(mut self, age: Positive) -> Self {
        self.max_age = Some(age);
        self
    }
}

type Turn<'a, 'e, 'd, const ID: u8> =
    turns::Parts<'a, 'e, 'd, conn::Transport, pool::Cursor, KeyTag<ID>>;
