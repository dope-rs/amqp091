use std::mem;

use o3::base::num::bounded;
use o3::mem::credit;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Slot {
    Delivery,
    Returned,
    Route,
}

impl From<Slot> for Resource {
    fn from(slot: Slot) -> Self {
        match slot {
            Slot::Delivery => Self::Delivery,
            Slot::Returned => Self::Returned,
            Slot::Route => Self::Route,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resource {
    Delivery,
    Returned,
    Body(credit::Error),
    Resident(credit::Error),
    Copy(credit::Error),
    Account(credit::Error),
    Fragment,
    Route,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Kind {
    Delivery,
    Returned,
    Body,
    Resident,
    Fragment,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::service::content) enum Entry {
    Delivery,
    Returned,
}

impl Entry {
    pub(super) const fn kind(self) -> Kind {
        match self {
            Self::Delivery => Kind::Delivery,
            Self::Returned => Kind::Returned,
        }
    }
}

impl Kind {
    pub(super) const COUNT: usize = Self::Fragment as usize + 1;

    pub(super) const fn index(self) -> bounded::U32<0, { Self::COUNT as u32 - 1 }> {
        match self {
            Self::Delivery => bounded::U32::exact::<0>(),
            Self::Returned => bounded::U32::exact::<1>(),
            Self::Body => bounded::U32::exact::<2>(),
            Self::Resident => bounded::U32::exact::<3>(),
            Self::Fragment => bounded::U32::exact::<4>(),
        }
    }
}

impl Resource {
    pub(super) const fn kind(self) -> Option<Kind> {
        match self {
            Self::Delivery => Some(Kind::Delivery),
            Self::Returned => Some(Kind::Returned),
            Self::Body(_) => Some(Kind::Body),
            Self::Resident(_) | Self::Copy(_) | Self::Account(_) => Some(Kind::Resident),
            Self::Fragment => Some(Kind::Fragment),
            Self::Route => None,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Slot>() == 1);
    assert!(mem::size_of::<Kind>() == 1);
    assert!(mem::size_of::<Entry>() == 1);
    assert!(mem::size_of::<Option<Entry>>() == 1);
    assert!(mem::size_of::<Resource>() == 3 * mem::size_of::<usize>());
};
