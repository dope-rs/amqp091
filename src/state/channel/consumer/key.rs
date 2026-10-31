use std::num;

use o3::depot::arena::hash::runtime;
use o3::depot::arena::slab::key::Handle;

use crate::state::channel::consumer;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Key(Handle<runtime::Domain<consumer::Tag>>);

impl Key {
    pub(super) const fn from_raw(raw: Handle<runtime::Domain<consumer::Tag>>) -> Self {
        Self(raw)
    }

    pub(super) const fn raw(self) -> Handle<runtime::Domain<consumer::Tag>> {
        self.0
    }

    pub(crate) const fn index(self) -> u32 {
        self.0.index()
    }
}

impl From<Key> for num::NonZero<u64> {
    fn from(value: Key) -> Self {
        value.0.parts().into_raw()
    }
}
