use o3::alloc;
use o3::depot::arena::pool::shared;
use o3::depot::capacity;

use crate::failure::terminal;

/// Fixed-capacity source of terminal-cause cells.
#[repr(transparent)]
pub(crate) struct Pool<'b, 'd>(shared::Store<terminal::Payload<'b, 'd>>);

impl<'b, 'd> Pool<'b, 'd> {
    pub(crate) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        shared::Store::try_new(capacity).map(Self)
    }

    pub(crate) fn reserve<'c>(&'c self) -> Option<terminal::Reservation<'c, 'b, 'd>> {
        let vacancy = self.0.vacancy()?;
        Some(terminal::Reservation::new(vacancy.reserve()))
    }
}
