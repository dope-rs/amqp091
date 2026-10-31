use o3::buffer::resident::Budget;

use crate::failure;
use crate::protocol::wire::bytes;
use crate::protocol::wire::text::short;

pub(crate) struct Delete<'d>(bytes::Field<'d>);

impl<'d> Delete<'d> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        name: short::Short<'_>,
    ) -> Result<Self, failure::Error> {
        Ok(Self(bytes::Field::capture(budget, name)?))
    }

    pub(in crate::protocol::topology) fn name(&self) -> short::Short<'_> {
        self.0.value()
    }
}
