use crate::protocol::topology::records::queue::Key;

#[derive(Clone, Copy)]
pub(crate) struct Cursor<'d, const ID: u8> {
    pub(in crate::protocol::topology) next: usize,
    pub(in crate::protocol::topology) current: Option<Key<'d, ID>>,
    pub(in crate::protocol::topology) current_replayed: bool,
}
