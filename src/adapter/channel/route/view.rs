use crate::protocol::topology;
use crate::state::conn::core::Core;
use crate::state::{channel, channels};

pub(in crate::adapter) struct View<'conn, 'a, 's, 'v, 'r, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(in crate::adapter) connection: &'conn Core<'c, 'b, 'd, ID>,
    pub(in crate::adapter) index: channels::Index<'c, ID>,
    pub(in crate::adapter) channel: &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
    pub(in crate::adapter) topology: &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
}

impl<'r, 'c: 'r, 'b: 'c, 'conn, 'a, 's, 'v, 'd: 'b, const ID: u8>
    View<'conn, 'a, 's, 'v, 'r, 'c, 'b, 'd, ID>
{
    pub(super) const fn new(
        conn: &'conn Core<'c, 'b, 'd, ID>,
        index: channels::Index<'c, ID>,
        channel: &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
        topology: &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
    ) -> Self {
        Self {
            connection: conn,
            index,
            channel,
            topology,
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<
        View<'static, 'static, 'static, 'static, 'static, 'static, 'static, 'static, 0>,
    >() == std::mem::size_of::<(
        &'static Core<'static, 'static, 'static, 0>,
        channels::Index<'static, 0>,
        &'static mut channel::Session<'static, 'static, 'static, 'static, 0>,
        &'static mut topology::State<'static, 'static, 'static, 'static, 0>,
    )>()
);
const _: () = assert!(
    std::mem::align_of::<
        View<'static, 'static, 'static, 'static, 'static, 'static, 'static, 'static, 0>,
    >() == std::mem::align_of::<(
        &'static Core<'static, 'static, 'static, 0>,
        channels::Index<'static, 0>,
        &'static mut channel::Session<'static, 'static, 'static, 'static, 0>,
        &'static mut topology::State<'static, 'static, 'static, 'static, 0>,
    )>()
);
