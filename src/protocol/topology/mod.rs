mod channel;
pub(crate) mod graph;
pub(crate) mod need;
mod plan;
pub(crate) mod records;
pub(crate) mod store;
mod work;

pub(crate) use channel::{Local, Refresh, State, Table};
use o3::depot::arena::slab::{self, entries};
pub(crate) use plan::Plan;
pub(crate) use work::Work;

use self::records::queue;

type Record<'c, 'b, 'd> = slab::Borrowed<
    entries::Busy<'c, queue::Declare<'b, 'd>, { u32::MAX }, entries::Interior<false>>,
    queue::Tag,
>;
