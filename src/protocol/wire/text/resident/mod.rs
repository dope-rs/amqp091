use std::hash::{Hash, Hasher};
use std::mem;

use o3::buffer::resident;
use o3::depot::arena::pool::{Shared, Unique, shared};
use o3::depot::capacity;
use o3::mem::rc;
use o3::{alloc, buffer};
pub use short::Short;

use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::text::{self, short::Buf};

mod raw;
mod short;

const CAPACITY: usize = 255;

#[derive(Debug)]
struct Inline {
    len: u8,
    bytes: [mem::MaybeUninit<u8>; CAPACITY],
    hash: u64,
}

impl Default for Inline {
    fn default() -> Self {
        Self {
            len: 0,
            bytes: [mem::MaybeUninit::uninit(); CAPACITY],
            hash: Fingerprint::new().get(),
        }
    }
}

impl Inline {
    const fn hash(&self) -> u64 {
        self.hash
    }
}

#[repr(transparent)]
pub(crate) struct Pool(shared::Store<Inline>);

#[must_use = "an identity writer must be committed, restored, or released"]
#[repr(transparent)]
pub(crate) struct Writer<'d>(
    Unique<'d, shared::Resident<Inline>, shared::Header<Inline>, shared::Initialized>,
);

#[must_use = "dropping the final identity lease recycles its slot"]
#[derive(Debug)]
#[repr(transparent)]
pub struct Identity<'d>(Shared<'d, Inline>);

impl Pool {
    pub(crate) fn try_new(cap: capacity::Count) -> Result<Self, alloc::Error> {
        shared::Store::try_new(cap).map(Self)
    }

    pub(crate) fn try_writer(&self) -> Option<Writer<'_>> {
        let vacancy = self.0.vacancy()?;
        Some(Writer(vacancy.reserve().begin(Inline::default())))
    }
}

impl<'d> Writer<'d> {
    pub(crate) fn commit_pair(
        mut self,
        value: text::short::Short<'_>,
        hash: u64,
    ) -> (Identity<'d>, Identity<'d>) {
        self.0.initialize(value, hash);
        let (first, second) = self.0.freeze_pair();
        (Identity(first), Identity(second))
    }

    pub(crate) fn commit(mut self, value: text::short::Short<'_>, hash: u64) -> Identity<'d> {
        self.0.initialize(value, hash);
        let lease = self.0.freeze();
        Identity(lease)
    }

    pub(crate) fn restore(self) -> Identity<'d> {
        let lease = self.0.freeze();
        Identity(lease)
    }

    pub(crate) fn hash(&self) -> u64 {
        self.0.hash()
    }
}

impl Identity<'_> {
    pub(crate) fn hash(&self) -> u64 {
        self.0.hash()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'d> Identity<'d> {
    pub(crate) fn try_writer(self) -> Result<Writer<'d>, Self> {
        match self.0.try_unique() {
            Ok(unique) => Ok(Writer(unique)),
            Err(lease) => Err(Self(lease)),
        }
    }
}

impl Identity<'_> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self(self.0.try_clone()?))
    }
}

impl PartialEq for Identity<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<Buf> for Identity<'_> {
    fn eq(&self, other: &Buf) -> bool {
        self.as_str() == other.as_ref()
    }
}

impl PartialEq<Exact<'_>> for Identity<'_> {
    fn eq(&self, other: &Exact<'_>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Identity<'_> {}

impl Hash for Identity<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

const _: () = {
    assert!(mem::size_of::<Inline>() == 33 * mem::size_of::<u64>());
    assert!(mem::size_of::<Pool>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Writer<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Identity<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Identity<'static>>>() == mem::size_of::<usize>());
};

#[doc(hidden)]
#[derive(Debug)]
#[repr(transparent)]
pub struct Exact<'d>(pub(in crate::protocol::wire::text) buffer::raw::Text<'d>);

impl<'d> Exact<'d> {
    pub(crate) fn from_owned(value: Buf) -> Self {
        let owner = resident::Exact::from_plain(value.0);
        // SAFETY: Buf validates UTF-8 before publishing its immutable owner.
        Self(unsafe { buffer::raw::utf8(owner) })
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Exact<'_> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self(self.0.try_clone()?))
    }
}

impl PartialEq for Exact<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<Identity<'_>> for Exact<'_> {
    fn eq(&self, other: &Identity<'_>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<Buf> for Exact<'_> {
    fn eq(&self, other: &Buf) -> bool {
        self.as_str() == other.as_ref()
    }
}

impl Eq for Exact<'_> {}

impl Hash for Exact<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

const _: () =
    assert!(mem::size_of::<Exact<'static>>() == mem::size_of::<resident::Exact<'static>>());
