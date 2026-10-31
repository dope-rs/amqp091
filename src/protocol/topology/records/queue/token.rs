use crate::protocol::topology::records::queue::Key;

#[derive(Clone, Copy)]
pub(crate) struct Token<'d, const ID: u8> {
    key: Key<'d, ID>,
    server_named: bool,
}

impl<'d, const ID: u8> Token<'d, ID> {
    pub(in crate::protocol) const fn new(key: Key<'d, ID>, server: bool) -> Self {
        Self {
            key,
            server_named: server,
        }
    }

    pub(crate) const fn key(self) -> Key<'d, ID> {
        self.key
    }

    pub(crate) const fn is_server(self) -> bool {
        self.server_named
    }
}
