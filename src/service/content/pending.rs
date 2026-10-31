use std::{marker, mem};

use o3::buffer::resident;
use o3::depot::arena::pool::{Unique, recycle};
use recycle::release;

use crate::adapter::channel::Returned;
use crate::failure::{self, wire};
use crate::message::field::view;
use crate::message::{content, property};
use crate::protocol::method::basic;
use crate::protocol::wire::schema;
use crate::protocol::wire::text::fields;
use crate::service::content::wait::wake::Wake;
use crate::service::content::wait::{resource, wake};
use crate::service::content::{entry, payload};
use crate::state::channel::consumer::delivery;
use crate::state::channel::returns;

pub(crate) enum Complete<'r, 'c, 'b, 'd> {
    Delivery(Option<delivery::Claim<'r, 'c, 'b, 'd>>),
    Returned(returns::Claim<'r, 'c, 'b, 'd>),
}

pub(crate) enum Error {
    Retry(resource::Resource),
    Failure(failure::Error),
}

pub(crate) enum Progress<'r, 'c, 'b, 'd, E> {
    Pending(Pending<'r, 'c, 'b, 'd>),
    Complete(Complete<'r, 'c, 'b, 'd>),
    Failed(Pending<'r, 'c, 'b, 'd>, E),
}

enum Route<'r, 'c, 'b, 'd> {
    Delivery(delivery::Vacant<'r, 'c, 'b, 'd>),
    Discard,
    Returned(returns::Vacant<'r, 'c, 'b, 'd>),
}

impl From<failure::Error> for Error {
    fn from(error: failure::Error) -> Self {
        Self::Failure(error)
    }
}

pub(crate) struct Pending<'r, 'c, 'b, 'd> {
    slot: Unique<
        'c,
        entry::Entry<'b, 'd>,
        release::Header<entry::Entry<'b, 'd>, wake::Wake<'b, 'd>>,
        release::Notify<'c>,
    >,
    route: Route<'r, 'c, 'b, 'd>,
}

/// Complete content retained under its matching route and frozen slot ownership.
pub(crate) struct Lease<'c, 'b, 'd, K>(
    Unique<
        'c,
        entry::Entry<'b, 'd>,
        release::Header<entry::Entry<'b, 'd>, wake::Wake<'b, 'd>>,
        release::Notify<'c>,
        release::Frozen,
    >,
    marker::PhantomData<K>,
);

impl<'r, 'c, 'b, 'd> Pending<'r, 'c, 'b, 'd> {
    pub(super) fn delivery<'input>(
        mut slot: Unique<
            'c,
            entry::Entry<'b, 'd>,
            release::Header<entry::Entry<'b, 'd>, wake::Wake<'b, 'd>>,
            release::Notify<'c>,
        >,
        method: basic::Deliver<'input>,
        route: delivery::Route<'r, 'c, 'b, 'd>,
    ) -> Self {
        let entry = slot.get_mut().get_mut();
        let text = fields::Packed::copy_from(&method.text, &mut entry.text);
        entry.kind = entry::Kind {
            delivery: entry::Delivery {
                delivery_tag: method.delivery_tag,
                redelivered: method.redelivered,
                text,
            },
        };
        let route = match route {
            delivery::Route::Queue(vacant) => Route::Delivery(vacant),
            delivery::Route::Discard => Route::Discard,
        };
        Self { slot, route }
    }

    pub(super) fn returned<'input>(
        mut slot: Unique<
            'c,
            entry::Entry<'b, 'd>,
            release::Header<entry::Entry<'b, 'd>, wake::Wake<'b, 'd>>,
            release::Notify<'c>,
        >,
        method: basic::Return<'input>,
        route: returns::Vacant<'r, 'c, 'b, 'd>,
    ) -> Self {
        let entry = slot.get_mut().get_mut();
        let text = fields::Packed::copy_from(&method.text, &mut entry.text);
        entry.kind = entry::Kind {
            returned: entry::Returned {
                reply_code: method.reply_code,
                text,
            },
        };
        Self {
            slot,
            route: Route::Returned(route),
        }
    }

    pub(crate) fn bind<E: From<failure::Error>>(
        mut self,
        wake: Wake<'b, 'd>,
        bind: impl FnOnce() -> Result<
            (
                property::Properties<resident::Shared<'d>, view::TableSpan>,
                bool,
                payload::Payload<'b, 'd>,
            ),
            E,
        >,
    ) -> Progress<'r, 'c, 'b, 'd, E> {
        if self.is_bound() {
            let error = E::from(failure::Error::Protocol(
                wire::Fault::ContentHeaderDuplicate {
                    properties: true,
                    body: true,
                },
            ));
            return Progress::Failed(self, error);
        }
        let (props, resident, body) = match bind() {
            Ok(bound) => bound,
            Err(error) => return Progress::Failed(self, error),
        };
        let entry = self.slot.get_mut().get_mut();
        entry.content = Some(entry::Content {
            properties: props,
            resident,
            body,
        });
        self.advance(wake)
    }

    pub(crate) fn is_bound(&self) -> bool {
        self.slot.get().get_ref().content.is_some()
    }

    pub(crate) fn body<E: From<failure::Error>>(
        mut self,
        length: usize,
        wake: Wake<'b, 'd>,
        store: impl FnOnce(&mut payload::Payload<'b, 'd>) -> Result<(), E>,
    ) -> Progress<'r, 'c, 'b, 'd, E> {
        let entry = self.slot.get_mut().get_mut();
        let content = match entry.content.as_mut() {
            Some(content) => content,
            None => {
                let error = E::from(failure::Error::Protocol(wire::Fault::ContentBodyUnbound {
                    actual: length,
                }));
                return Progress::Failed(self, error);
            }
        };
        if let Err(error) = store(&mut content.body) {
            return Progress::Failed(self, error);
        }
        self.advance(wake)
    }

    fn advance<E>(self, wake: Wake<'b, 'd>) -> Progress<'r, 'c, 'b, 'd, E> {
        let Self { slot, route } = self;
        let content = match slot.get().get_ref().content.as_ref() {
            Some(content) if content.body.remaining() == 0 => content,
            _ => return Progress::Pending(Self { slot, route }),
        };
        let release = content.body.release();
        wake.complete(
            release.bytes,
            release.fragments,
            content.resident || release.resident,
        );
        let slot = slot.commit();
        Progress::Complete(match route {
            Route::Delivery(route) => {
                Complete::Delivery(Some(route.insert(Lease(slot, marker::PhantomData))))
            }
            Route::Discard => {
                drop(slot);
                Complete::Delivery(None)
            }
            Route::Returned(route) => Complete::Returned(
                route.insert(Returned::from_lease(Lease(slot, marker::PhantomData))),
            ),
        })
    }
}

impl Lease<'_, '_, '_, content::DeliveryText> {
    fn delivery(&self) -> &entry::Delivery {
        // SAFETY: only the matching Route arm mints this type, after its
        // constructor wrote this member. Frozen preserves it for this loan.
        unsafe { &self.entry().kind.delivery }
    }

    pub(crate) fn delivery_tag(&self) -> u64 {
        self.delivery().delivery_tag
    }

    pub(crate) fn is_repeat(&self) -> bool {
        self.delivery().redelivered
    }

    pub(crate) fn delivery_str<const INDEX: usize>(
        &self,
        field: schema::Field<content::DeliveryText, INDEX>,
    ) -> &str {
        self.delivery().text.get(&self.entry().text, field)
    }
}

impl Lease<'_, '_, '_, content::ReturnText> {
    fn returned(&self) -> &entry::Returned {
        // SAFETY: only the matching Route arm mints this type, after its
        // constructor wrote this member. Frozen preserves it for this loan.
        unsafe { &self.entry().kind.returned }
    }

    pub(crate) fn reply_code(&self) -> u16 {
        self.returned().reply_code
    }

    pub(crate) fn return_text<const INDEX: usize>(
        &self,
        field: schema::Field<content::ReturnText, INDEX>,
    ) -> &str {
        self.returned().text.get(&self.entry().text, field)
    }
}

impl<'c, 'b, 'd, K> Lease<'c, 'b, 'd, K> {
    fn entry(&self) -> &entry::Entry<'b, 'd> {
        self.0.get().get_ref()
    }

    fn value(&self) -> &entry::Content<'b, 'd> {
        // SAFETY: Pending::advance checks completion before creating this
        // lease. Frozen retains that Some until this owner's final release.
        unsafe { &*self.entry().content.as_slice().as_ptr() }
    }

    pub(crate) fn properties(&self) -> content::Properties<'_, 'd> {
        content::Properties::new(&self.value().properties)
    }

    pub(crate) fn body(&self) -> content::Body<'_, 'b, 'd> {
        content::Body::new(self)
    }

    pub(crate) fn payload(&self) -> &payload::Payload<'b, 'd> {
        &self.value().body
    }

    pub(crate) fn release(self) {
        drop(self);
    }
}

const _: () = assert!(
    mem::size_of::<Pending<'static, 'static, 'static, 'static>>() == 4 * mem::size_of::<usize>()
);

const _: () = {
    assert!(
        size_of::<Lease<'static, 'static, 'static, content::DeliveryText>>() == size_of::<usize>()
    );
    assert!(
        size_of::<Option<Lease<'static, 'static, 'static, content::DeliveryText>>>()
            == size_of::<usize>()
    );
    assert!(
        size_of::<Lease<'static, 'static, 'static, content::ReturnText>>() == size_of::<usize>()
    );
    assert!(
        size_of::<Option<Lease<'static, 'static, 'static, content::ReturnText>>>()
            == size_of::<usize>()
    );
};
