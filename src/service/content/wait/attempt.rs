use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::ready;
use dope::manifold::client::conn;

use crate::service::content::wait::resource::Resource;

/// Move-only authority to suspend the exact input currently being delivered.
pub(crate) struct Attempt<'d, const ID: u8> {
    connection: conn::Id<'d, KeyTag<ID>>,
    target: ready::Target<'d>,
}

/// One failed admission bound to its physical connection generation.
#[must_use = "a blocked input must be registered before returning to the driver"]
pub(crate) struct Blocked<'d, const ID: u8> {
    connection: conn::Id<'d, KeyTag<ID>>,
    target: ready::Target<'d>,
    resource: Resource,
}

impl<'d, const ID: u8> Attempt<'d, ID> {
    pub(crate) const fn new(conn: conn::Id<'d, KeyTag<ID>>, target: ready::Target<'d>) -> Self {
        Self {
            connection: conn,
            target,
        }
    }

    pub(crate) const fn block(self, resource: Resource) -> Blocked<'d, ID> {
        Blocked {
            connection: self.connection,
            target: self.target,
            resource,
        }
    }
}

impl<'d, const ID: u8> Blocked<'d, ID> {
    pub(super) const fn into_parts(
        self,
    ) -> (conn::Id<'d, KeyTag<ID>>, ready::Target<'d>, Resource) {
        (self.connection, self.target, self.resource)
    }
}

const _: () = {
    assert!(
        size_of::<Attempt<'static, 0>>()
            == size_of::<conn::Id<'static, KeyTag<0>>>() + size_of::<ready::Target<'static>>()
    );
    assert!(
        size_of::<Blocked<'static, 0>>()
            == size_of::<Attempt<'static, 0>>() + size_of::<Resource>()
    );
};
