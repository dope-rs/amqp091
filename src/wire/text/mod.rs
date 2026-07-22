use std::fmt;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::str::from_utf8_unchecked;

use o3::buffer::Shared;

#[derive(Clone)]
pub struct SharedStr(Shared);

impl SharedStr {
    pub(crate) fn new(bytes: Shared) -> Self {
        Self(bytes)
    }
}

impl From<String> for SharedStr {
    fn from(value: String) -> Self {
        Self(Shared::from(value))
    }
}

impl From<&str> for SharedStr {
    fn from(value: &str) -> Self {
        Self(Shared::from(value))
    }
}

impl Deref for SharedStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe { from_utf8_unchecked(self.0.as_slice()) }
    }
}

impl AsRef<str> for SharedStr {
    fn as_ref(&self) -> &str {
        self
    }
}

impl fmt::Debug for SharedStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_ref(), f)
    }
}

impl fmt::Display for SharedStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self.as_ref(), f)
    }
}

impl PartialEq<str> for SharedStr {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<&str> for SharedStr {
    fn eq(&self, other: &&str) -> bool {
        self.as_ref() == *other
    }
}
