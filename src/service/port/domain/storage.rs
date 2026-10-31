use o3::buffer::resident;

use crate::failure::terminal;
use crate::protocol::wire::text::resident::Pool;

pub(in crate::service) struct Storage<'c, 'b, 'd> {
    pub(in crate::service::port) budget: resident::Budget<'d>,
    pub(in crate::service::port) identities: &'b Pool,
    pub(in crate::service::port) causes: &'c terminal::Pool<'b, 'd>,
}
