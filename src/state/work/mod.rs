pub(crate) mod requests;

use std::mem;

use crate::protocol::topology::records::consumer::Record;
use crate::protocol::topology::{self, need};

pub(crate) enum Admit<'c, 'b, 'd, const ID: u8> {
    Direct(need::IdentityPlan),
    Topology {
        identity: need::IdentityPlan,
        work: topology::Work<'c, 'b, 'd, ID>,
    },
    Consumer(Record<'b, 'd, ID>),
}

const _: () = assert!(mem::size_of::<Admit<'_, '_, '_, 0>>() <= 7 * mem::size_of::<usize>());
