use crate::state::channel::methods::MethodKey;

/// A revalidated hint that the keyed control operation may be next.
#[repr(transparent)]
pub(crate) struct Next {
    key: MethodKey,
}

impl Next {
    pub(in crate::state::channel::methods) const fn new(key: MethodKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> MethodKey {
        self.key
    }
}

const _: () = assert!(std::mem::size_of::<Next>() == std::mem::size_of::<MethodKey>());
