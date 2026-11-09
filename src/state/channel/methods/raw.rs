use o3::depot;

use crate::state::channel::methods::MethodKey;

// SAFETY: MethodKey is a transparent, identity-preserving slab handle wrapper.
unsafe impl depot::raw::Address for MethodKey {
    fn index(self) -> usize {
        depot::raw::Address::index(self.raw())
    }
}
