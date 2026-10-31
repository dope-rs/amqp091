use std::hash;

use o3::mem::rc;

use crate::protocol::wire::text::{self, resident};

#[derive(Debug)]
pub(super) enum Inner<'b, 'd> {
    Exact(resident::Exact<'d>),
    Identity(resident::Identity<'b>),
    Capsule(text::raw::Capsule<'d>),
}

#[derive(Debug)]
pub struct Short<'b, 'd>(pub(super) Inner<'b, 'd>);

impl AsRef<str> for Short<'_, '_> {
    fn as_ref(&self) -> &str {
        match &self.0 {
            Inner::Exact(value) => value.as_str(),
            Inner::Identity(value) => value.as_str(),
            Inner::Capsule(value) => value.as_str(),
        }
    }
}

impl<'b, 'd> Short<'b, 'd> {
    pub fn identity(&self) -> Option<&resident::Identity<'b>> {
        match &self.0 {
            Inner::Identity(value) => Some(value),
            Inner::Exact(_) | Inner::Capsule(_) => None,
        }
    }
}

impl<'b, 'd> From<resident::Exact<'d>> for Short<'b, 'd> {
    fn from(value: resident::Exact<'d>) -> Self {
        Self(Inner::Exact(value))
    }
}

impl<'b, 'd> From<resident::Identity<'b>> for Short<'b, 'd> {
    fn from(value: resident::Identity<'b>) -> Self {
        Self(Inner::Identity(value))
    }
}

impl<'b, 'd> From<text::raw::Capsule<'d>> for Short<'b, 'd> {
    fn from(value: text::raw::Capsule<'d>) -> Self {
        Self(Inner::Capsule(value))
    }
}

impl Short<'_, '_> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self(match &self.0 {
            Inner::Exact(value) => Inner::Exact(value.try_clone()?),
            Inner::Identity(value) => Inner::Identity(value.try_clone()?),
            Inner::Capsule(value) => Inner::Capsule(value.try_clone()?),
        }))
    }
}

impl PartialEq for Short<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Short<'_, '_> {}

impl hash::Hash for Short<'_, '_> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

const _: () =
    assert!(std::mem::size_of::<Short<'static, 'static>>() == 3 * std::mem::size_of::<usize>());
