use std::{hash, mem, num::NonZero, str};

use o3::buffer::resident::copy;
use o3::buffer::{self, storage};

use crate::failure::{self, wire};
use crate::protocol::wire::text::resident;

pub(in crate::protocol) mod raw;

/// Admits a short string at compile time and rejects invalid literals.
#[macro_export]
macro_rules! short_literal {
    ($value:expr $(,)?) => {
        const {
            match $value {
                ::core::result::Result::Ok(value) => value,
                ::core::result::Result::Err(_) => ::core::panic!("short"),
            }
        }
    };
}

pub use crate::short_literal as literal;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Length { actual: usize },
    Nul { offset: usize },
}

impl Error {
    fn into_error(self, kind: Resource, max: usize) -> failure::Error {
        match self {
            Self::Length { actual } => kind.too_long(actual, max),
            Self::Nul { offset } => failure::Error::Protocol(wire::Fault::TextNul {
                kind: kind.encoding(),
                offset,
            }),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Short<'a>(pub(in crate::protocol::wire::text) &'a str);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Name<'a>(Short<'a>);

pub(crate) struct InlineName {
    bytes: [mem::MaybeUninit<u8>; Name::MAX as usize],
    len: u8,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    Short,
    Queue,
    Exchange,
    VirtualHost,
    BrokerQueue,
    BrokerConsumer,
}

impl Resource {
    const fn encoding(self) -> wire::Encoding {
        match self {
            Self::Short => wire::Encoding::Short,
            Self::Queue => wire::Encoding::Queue,
            Self::Exchange => wire::Encoding::Exchange,
            Self::VirtualHost => wire::Encoding::VirtualHost,
            Self::BrokerQueue => wire::Encoding::BrokerQueue,
            Self::BrokerConsumer => wire::Encoding::BrokerConsumer,
        }
    }

    fn too_long(self, len: usize, max: usize) -> failure::Error {
        failure::Error::Encoding(wire::Encode::Length {
            kind: self.encoding(),
            actual: len,
            max,
        })
    }

    fn allocation(self, error: storage::AllocError) -> failure::Error {
        let target = match self {
            Self::Short => wire::Allocation::Short,
            Self::Queue => wire::Allocation::Queue,
            Self::Exchange => wire::Allocation::Exchange,
            Self::VirtualHost => wire::Allocation::VirtualHost,
            Self::BrokerQueue => wire::Allocation::BrokerQueue,
            Self::BrokerConsumer => wire::Allocation::BrokerConsumer,
        };
        failure::Error::storage(target, error)
    }
}

impl<'a> Short<'a> {
    pub const EMPTY: Self = Self("");

    pub fn new(value: &'a str) -> Result<Self, failure::Error> {
        Self::try_new(value).map_err(|error| error.into_error(Resource::Short, u8::MAX as usize))
    }

    pub const fn try_new(value: &'a str) -> Result<Self, Error> {
        Self::check(value, u8::MAX as usize)
    }

    pub fn resource(value: &'a str, kind: Resource) -> Result<Name<'a>, failure::Error> {
        Self::check(value, usize::from(Name::MAX))
            .map(Name)
            .map_err(|error| error.into_error(kind, usize::from(Name::MAX)))
    }

    pub fn value(self) -> &'a str {
        self.0
    }

    pub fn as_str(&self) -> &'a str {
        self.0
    }

    pub fn len(self) -> u8 {
        self.0.len() as u8
    }

    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn try_copy<'d>(
        &self,
        target: wire::Allocation,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<resident::Exact<'d>, failure::Error> {
        let owner = match NonZero::<u32>::new(u32::from(self.len())) {
            None => buffer::resident::Exact::new(),
            Some(capacity) => {
                // SAFETY: Short retains UTF-8 and the exact u8 length bound.
                let slices =
                    unsafe { buffer::raw::Slices::new(capacity, [self.value().as_bytes()]) };
                slices.try_copy(budget).map_err(|error| match error {
                    copy::Error::Budget(error) => failure::Error::resident(
                        target,
                        buffer::resident::AllocError::Budget(error),
                    ),
                    copy::Error::Allocation(error) => failure::Error::Exhausted { target, error },
                })?
            }
        };
        // SAFETY: copying the validated text preserves UTF-8 and its short-string bounds.
        Ok(resident::Exact(unsafe { buffer::raw::utf8(owner) }))
    }

    const fn check(value: &'a str, max: usize) -> Result<Self, Error> {
        if value.len() > max {
            return Err(Error::Length {
                actual: value.len(),
            });
        }
        let bytes = value.as_bytes();
        let mut offset = 0;
        while offset < bytes.len() {
            if bytes[offset] == 0 {
                return Err(Error::Nul { offset });
            }
            offset += 1;
        }
        Ok(Self(value))
    }
}

impl<'a> Name<'a> {
    const MAX: u8 = 127;

    pub const EMPTY: Self = Self(Short::EMPTY);

    pub(in crate::protocol) fn new(
        value: Short<'a>,
        kind: Resource,
    ) -> Result<Self, failure::Error> {
        if value.len() > Self::MAX {
            return Err(kind.too_long(usize::from(value.len()), usize::from(Self::MAX)));
        }
        Ok(Self(value))
    }

    pub fn value(self) -> &'a str {
        self.0.value()
    }

    pub fn as_str(&self) -> &'a str {
        self.0.as_str()
    }

    pub fn len(self) -> u8 {
        self.0.len()
    }

    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> From<Name<'a>> for Short<'a> {
    fn from(value: Name<'a>) -> Self {
        value.0
    }
}

impl<'a> From<&'a resident::Short<'_, '_>> for Short<'a> {
    fn from(value: &'a resident::Short<'_, '_>) -> Self {
        Self(value.as_ref())
    }
}

impl<'a> From<&'a resident::Exact<'_>> for Short<'a> {
    fn from(value: &'a resident::Exact<'_>) -> Self {
        Self(value.as_str())
    }
}

impl<'a> From<&'a resident::Identity<'_>> for Short<'a> {
    fn from(value: &'a resident::Identity<'_>) -> Self {
        Self(value.as_str())
    }
}

const _: () = {
    assert!(std::mem::size_of::<Short<'static>>() == 2 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Name<'static>>() == std::mem::size_of::<Short<'static>>());
};

#[repr(transparent)]
#[derive(Debug)]
pub struct Buf(pub(in crate::protocol::wire::text) storage::Exact);

impl AsRef<str> for Buf {
    fn as_ref(&self) -> &str {
        let bytes = self.0.as_slice();
        // SAFETY: every constructor validates UTF-8 before binding immutable backing bytes.
        unsafe { str::from_utf8_unchecked(bytes) }
    }
}

impl Buf {
    pub fn new(value: impl AsRef<str>) -> Result<Self, failure::Error> {
        Self::try_copy(Short::new(value.as_ref())?, Resource::Short)
    }

    pub fn resource(value: impl AsRef<str>, kind: Resource) -> Result<Self, failure::Error> {
        Self::try_copy(Short::resource(value.as_ref(), kind)?.into(), kind)
    }

    pub(crate) fn try_copy(value: Short<'_>, kind: Resource) -> Result<Self, failure::Error> {
        let bytes = storage::Exact::try_copy(value.value().as_bytes())
            .map_err(|error| kind.allocation(error))?;
        Ok(Self(bytes))
    }

    pub fn as_short(&self) -> Short<'_> {
        let bytes = self.0.as_slice();
        // SAFETY: every constructor validates UTF-8 before binding immutable backing bytes.
        Short(unsafe { str::from_utf8_unchecked(bytes) })
    }
}

impl PartialEq for Buf {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}

impl Eq for Buf {}

impl hash::Hash for Buf {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.0.as_slice().hash(state);
    }
}

const _: () = assert!(std::mem::size_of::<Buf>() == std::mem::size_of::<storage::Exact>());
