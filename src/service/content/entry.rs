use std::{mem, pin};

use o3::buffer::resident;
use o3::depot::arena::pool::recycle;

use crate::message::field::view;
use crate::message::{content, property};
use crate::protocol::wire::text::fields;
use crate::service::content::payload;
use crate::service::content::wait::{release, resource};

#[derive(Clone, Copy)]
pub(super) union Kind {
    pub(super) empty: (),
    pub(super) delivery: Delivery,
    pub(super) returned: Returned,
}

#[derive(Clone, Copy)]
pub(super) struct Delivery {
    pub(super) delivery_tag: u64,
    pub(super) redelivered: bool,
    pub(super) text: fields::Packed<content::DeliveryText>,
}

#[derive(Clone, Copy)]
pub(super) struct Returned {
    pub(super) reply_code: u16,
    pub(super) text: fields::Packed<content::ReturnText>,
}

const _: () = assert!(mem::size_of::<Kind>() == mem::size_of::<Option<Delivery>>());

pub(super) struct Content<'b, 'd> {
    pub(super) properties: property::Properties<resident::Shared<'d>, view::TableSpan>,
    pub(super) resident: bool,
    pub(super) body: payload::Payload<'b, 'd>,
}

pub(super) struct Entry<'b, 'd> {
    pub(super) kind: Kind,
    pub(super) content: Option<Content<'b, 'd>>,
    pub(super) text: [u8; fields::PACKED_CAPACITY],
    resource: resource::Entry,
}

impl Entry<'_, '_> {
    fn release(&self) -> release::Release {
        let (property, payload) = match self.content.as_ref() {
            Some(content) => (content.resident, Some(content.body.release())),
            None => (false, None),
        };
        release::Release::entry(self.resource, property, payload)
    }
}

const _: () = assert!(
    mem::size_of::<Option<Content<'static, 'static>>>()
        <= mem::size_of::<(
            Option<property::Properties<resident::Shared<'static>, view::TableSpan>>,
            bool,
            Option<payload::Payload<'static, 'static>>,
        )>()
);

impl recycle::Recycle for Entry<'_, '_> {
    type Seed = resource::Entry;
    type Release = release::Release;

    fn from_seed(resource: Self::Seed) -> Self {
        Self {
            kind: Kind { empty: () },
            content: None,
            text: [0; fields::PACKED_CAPACITY],
            resource,
        }
    }

    fn recycle(self: pin::Pin<&mut Self>) -> Self::Release {
        let entry = self.get_mut();
        let release = entry.release();
        entry.content = None;
        release
    }
}
