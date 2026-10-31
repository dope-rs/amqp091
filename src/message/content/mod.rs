use std::{mem, ops};

use o3::buffer::resident;

use crate::message::field::view;
use crate::message::property;

mod body;
mod texts;

pub use body::Body;
pub(crate) use texts::{DeliveryText, ReturnText};

/// Properties retained from an inbound frame under the driver lifetime.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Properties<'a, 'd>(&'a property::Properties<resident::Shared<'d>, view::TableSpan>);

impl<'a, 'd> Properties<'a, 'd> {
    pub(crate) const fn new(
        props: &'a property::Properties<resident::Shared<'d>, view::TableSpan>,
    ) -> Self {
        Self(props)
    }

    pub const fn as_ref(&self) -> &'a property::Properties<resident::Shared<'d>, view::TableSpan> {
        self.0
    }
}

impl<'d> ops::Deref for Properties<'_, 'd> {
    type Target = property::Properties<resident::Shared<'d>, view::TableSpan>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Properties<'static, 'static>>() == mem::size_of::<usize>());
