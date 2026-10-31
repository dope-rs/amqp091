mod item;

pub(crate) use item::Item;

use crate::protocol::wire::text::resident;

pub(crate) enum Completion<'b, 'd> {
    Active,
    Retiring,
    Canceled(resident::Short<'b, 'd>),
}
