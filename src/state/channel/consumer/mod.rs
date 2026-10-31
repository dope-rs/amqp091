pub(crate) mod access;
pub(crate) mod delivery;
mod key;
mod plan;
pub(crate) mod replay;
mod rollback;
mod set;
mod storage;

pub(crate) use key::Key;
pub(crate) use plan::Plan;
pub(crate) use rollback::Rollback;
pub(crate) use set::{Removal, Retire, Set};

pub(crate) enum Cancel<'b, 'd> {
    Tracked(Key),
    Orphan(resident::Short<'b, 'd>),
}

pub(crate) enum Mismatch<'b, 'd> {
    Tag(resident::Short<'b, 'd>),
    Collision(turn::Clash),
}

impl<'b, 'd> Mismatch<'b, 'd> {
    pub(crate) fn try_error(
        self,
        actual: impl FnOnce() -> Result<resident::Short<'b, 'd>, failure::Error>,
    ) -> Result<failure::Error<resident::Short<'b, 'd>>, failure::Error> {
        match self {
            Self::Tag(expected) => Ok(failure::Error::ConsumerTag {
                expected,
                actual: actual()?,
            }),
            Self::Collision(error) => Ok(failure::Error::ConsumerCollision {
                error,
                tag: actual()?,
            }),
        }
    }
}

#[derive(Debug)]
pub(super) enum Tag {}

const _: () = assert!(
    mem::size_of::<Mismatch<'static, 'static>>()
        == mem::size_of::<resident::Short<'static, 'static>>()
);
use std::mem;

use crate::failure;
use crate::failure::wire::turn;
use crate::protocol::wire::text::resident;
