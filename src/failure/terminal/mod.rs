use o3::depot::arena::pool::{Unique, shared};

use crate::failure;
use crate::protocol::wire::text::resident;

mod lease;
mod outcome;
mod payload;
mod pool;
mod state;

pub(crate) use lease::Lease;
pub(crate) use outcome::Outcome;
pub(super) use payload::Payload;
pub(crate) use pool::Pool;
pub(crate) use state::State;

#[repr(transparent)]
pub(crate) struct Reservation<'c, 'b, 'd>(Unique<'c, shared::Resident<Payload<'b, 'd>>>);

impl<'c, 'b, 'd> Reservation<'c, 'b, 'd> {
    pub(super) const fn new(slot: Unique<'c, shared::Resident<Payload<'b, 'd>>>) -> Self {
        Self(slot)
    }

    pub(crate) fn commit(
        self,
        version: u64,
        error: failure::Error<resident::Short<'b, 'd>>,
    ) -> Lease<'c, 'b, 'd> {
        Lease::new(self.0.commit(Payload {
            generation: version,
            error,
        }))
    }
}
