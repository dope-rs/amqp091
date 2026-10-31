use std::num;

use o3::depot::queue::resident::fifo;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Index<'c, const ID: u8>(fifo::Compact<'c>);

impl<'c, const ID: u8> Index<'c, ID> {
    pub(super) fn try_new(index: fifo::Index<'c>) -> Result<Self, num::TryFromIntError> {
        Ok(Self(index.try_into()?))
    }

    pub(crate) const fn offset(self) -> u16 {
        self.0.get().get() - 1
    }

    pub(super) const fn position(self) -> num::NonZero<u16> {
        self.0.get()
    }

    pub(super) fn after(self, previous: Option<Self>) -> Option<Self> {
        self.0.after(previous.map(|index| index.0)).map(Self)
    }

    pub(crate) fn lane(self) -> fifo::Index<'c> {
        self.0.into_index()
    }
}

const _: () = {
    assert!(size_of::<Index<'static, 0>>() == size_of::<num::NonZero<u16>>());
    assert!(size_of::<Option<Index<'static, 0>>>() == size_of::<num::NonZero<u16>>());
};
