use crate::protocol::topology::records::consumer::Record;
use crate::state::channel::consumer::{self};

pub(crate) struct Item<'a, 'b, 'd, const ID: u8> {
    cursor: usize,
    token: consumer::Key,
    spec: &'a Record<'b, 'd, ID>,
}

impl<'b, 'a, 'd, const ID: u8> Item<'a, 'b, 'd, ID> {
    pub(in crate::state::channel::consumer) fn new(
        index: usize,
        token: consumer::Key,
        spec: &'a Record<'b, 'd, ID>,
    ) -> Self {
        Self {
            cursor: index,
            token,
            spec,
        }
    }

    pub(crate) const fn cursor(&self) -> usize {
        self.cursor
    }

    pub(crate) const fn spec(&self) -> &'a Record<'b, 'd, ID> {
        self.spec
    }

    pub(crate) const fn token(&self) -> consumer::Key {
        self.token
    }
}
