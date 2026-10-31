use crate::failure;
use crate::protocol::wire::text::resident;

pub enum Admit<'c, 'b: 'c, 'd: 'b, T, const ID: u8> {
    Item(T),
    Full,
    Error(failure::Runtime<'c, 'b, 'd, ID>),
}

pub(in crate::adapter) enum Reject<'b, 'd: 'b, D> {
    Error(failure::Error<resident::Short<'b, 'd>>),
    Defer(D),
}

impl<'b, 'd: 'b, D> From<failure::Error> for Reject<'b, 'd, D> {
    fn from(error: failure::Error) -> Self {
        Self::Error(error.into())
    }
}
