use std::num;

use o3::depot::arena::slab::key;

use crate::protocol::topology::records::queue::Tag;
use crate::protocol::topology::store;

#[doc(hidden)]
#[repr(transparent)]
pub(crate) struct Key<'d, const ID: u8> {
    raw: key::Handle<Tag>,
    owner: store::Owner<'d, ID>,
}

impl<'d, const ID: u8> Key<'d, ID> {
    pub(in crate::protocol::topology) const fn from_parts(
        raw: key::Handle<Tag>,
        owner: store::Owner<'d, ID>,
    ) -> Self {
        Self { raw, owner }
    }

    pub(in crate::protocol::topology) const fn into_parts(
        self,
    ) -> (key::Handle<Tag>, store::Owner<'d, ID>) {
        let Self { raw, owner } = self;
        (raw, owner)
    }

    pub(in crate::protocol::topology) const fn index(self) -> u32 {
        self.raw.index()
    }

    pub(in crate::protocol::topology) const fn generation(self) -> key::Generation {
        self.raw.generation()
    }

    pub(crate) const fn identity(self) -> num::NonZero<u64> {
        self.raw.parts().into_raw()
    }
}

impl<const ID: u8> Clone for Key<'_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<const ID: u8> Copy for Key<'_, ID> {}

impl<const ID: u8> PartialEq for Key<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<const ID: u8> Eq for Key<'_, ID> {}

const _: () = {
    assert!(std::mem::size_of::<Key<'static, 0>>() == std::mem::size_of::<u64>());
    assert!(std::mem::align_of::<Key<'static, 0>>() == std::mem::align_of::<u64>());
    assert!(std::mem::size_of::<Option<Key<'static, 0>>>() == std::mem::size_of::<u64>());
};
