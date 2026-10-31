use o3::depot;
use o3::depot::arena::slab::key;
use o3::depot::index::Stored;

use crate::failure::wire::turn::Request;
use crate::state::channel::methods::MethodKey;

// SAFETY: MethodKey is a transparent, identity-preserving slab handle wrapper.
unsafe impl depot::raw::Key for MethodKey {
    type Raw = <key::Handle<Request> as depot::raw::Key>::Raw;
    type Live = Self;

    const VACANT: Self::Raw = <key::Handle<Request> as depot::raw::Key>::VACANT;

    fn index(self) -> usize {
        depot::raw::Key::index(self.raw())
    }

    fn into_raw(self) -> Self::Raw {
        depot::raw::Key::into_raw(self.raw())
    }

    fn from_raw(raw: Stored<Self>) -> Self {
        // SAFETY: MethodKey and its Handle emit the same live identities;
        // wrapping the rebuilt Handle restores the exact original MethodKey.
        unsafe { Self::from_raw(<key::Handle<Request> as depot::raw::Key>::rebuild(raw)) }
    }

    fn decode(raw: Self::Raw) -> Option<Self> {
        <key::Handle<Request> as depot::raw::Key>::decode(raw).map(Self::from_raw)
    }

    fn into_live(self) -> Self::Live {
        self
    }

    fn from_live(live: Self::Live) -> Self {
        live
    }
}
