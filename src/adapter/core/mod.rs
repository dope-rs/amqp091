use std::marker;

use dope::manifold::client::close;
use o3::base::cell::{local, region};
use o3::buffer::resident;

use crate::adapter::channel;
use crate::protocol::wire::text::resident::Pool;
use crate::state::channel::methods::{self, send};
use crate::state::channel::{Freed, Stores};
use crate::state::{channels, epoch, shared};
use crate::{config, failure, permits, service};

mod access;
mod admit;
mod cancel;
pub(in crate::adapter) mod egress;
mod gate;

pub(in crate::adapter) use access::{
    Allows, Capability, Channels, Command, Conn, Consumers, Control, Egress, Issue, Methods,
    Observe, Project, Publish, Reap, Registry, Root, Settings, State, WaiterAccess,
};
pub(in crate::adapter) use admit::{Admit, Reject};
pub(in crate::adapter) use cancel::Cancel;
pub(in crate::adapter) use gate::Gate;

/// One-pointer typed lens into the route domain.
/// Its zero-sized marker changes authority without changing layout.
#[repr(transparent)]
pub(super) struct Core<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: Capability = Root> {
    domain: &'o service::Domain<'r, 'c, 'b, 'd, ID>,
    capability: marker::PhantomData<fn() -> K>,
}

impl<const ID: u8, K: Capability> Copy for Core<'_, '_, '_, '_, '_, ID, K> {}

impl<const ID: u8, K: Capability> Clone for Core<'_, '_, '_, '_, '_, ID, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Core<'o, 'r, 'c, 'b, 'd, ID, Root> {
    pub(super) const fn new(port: &'o service::Port<'r, 'c, 'b, 'd, ID>) -> Self {
        Self {
            domain: port.domain(),
            capability: marker::PhantomData,
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: Capability>
    Core<'o, 'r, 'c, 'b, 'd, ID, K>
{
    pub(super) const fn project<T: Capability>(self) -> Core<'o, 'r, 'c, 'b, 'd, ID, T>
    where
        K: Allows<Project<T>>,
    {
        Core {
            domain: self.domain,
            capability: marker::PhantomData,
        }
    }

    pub(super) const fn outbound(self) -> service::Admission<'o, 'r, 'c, 'b, 'd, ID>
    where
        K: Allows<State> + Allows<Egress>,
    {
        service::Admission::new(self.domain)
    }

    pub(super) const fn budget(self) -> &'o resident::Budget<'d> {
        self.domain.budget()
    }

    pub(super) const fn identities(self) -> &'b Pool {
        self.domain.identities()
    }

    pub(super) const fn shared(self) -> &'o shared::Root<'r, 'c, 'b, 'd, ID>
    where
        K: Allows<State>,
    {
        &self.domain.session
    }

    pub(super) fn stores(self) -> Stores<'r, 'c, 'b, 'd>
    where
        K: Allows<State>,
    {
        self.domain.content.stores()
    }

    pub(super) fn is_active(self, target: epoch::Id<'d, ID>) -> bool
    where
        K: Allows<State>,
    {
        self.domain.transport().is_active(target.token())
    }

    pub(super) fn close(self, target: epoch::Id<'d, ID>, rule: close::Directive)
    where
        K: Allows<State>,
    {
        self.domain.transport().close(target.token(), rule);
    }

    pub(super) fn waiters(self) -> &'o service::Waiters<'d>
    where
        K: Allows<WaiterAccess>,
    {
        self.domain.coordination().waiters()
    }

    pub(super) fn permits(self) -> &'o permits::Registry<'c, 'd>
    where
        K: Allows<Registry>,
    {
        self.domain.admission()
    }

    pub(super) fn config(self) -> &'o config::Config
    where
        K: Allows<Settings>,
    {
        self.domain.config()
    }

    pub(super) fn wake_route(
        self,
        target: epoch::Id<'d, ID>,
        freed: Freed,
    ) -> Result<(), failure::Error> {
        self.domain.content.wake_route(target.token(), freed)
    }

    pub(super) const fn cancel(self) -> Cancel<'o, 'd, ID, K> {
        Cancel::new(self.domain.coordination())
    }

    #[inline(never)]
    pub(super) fn flow(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: channels::Lease<'d, ID>,
        active: bool,
    ) where
        K: Allows<State> + Allows<Egress>,
    {
        if let Err(cause) = self.domain.flow(access.reborrow(), target, lease, active) {
            self.domain.fail(access, target, cause);
        }
    }

    pub(super) fn send(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: channels::Lease<'d, ID>,
        next: send::Next,
    ) where
        K: Allows<State> + Allows<Egress>,
    {
        let result = self
            .domain
            .control()
            .write(target, lease)
            .send(access.reborrow(), next);
        if let Err(cause) = result {
            self.domain.fail(access, target, cause);
        }
    }

    pub(super) fn detach(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        lease: channel::Lease<'c, 'd, ID>,
        action: methods::DetachAction,
    ) where
        K: Allows<Control>,
    {
        match action {
            methods::DetachAction::RetireConsumer(key) => {
                self.domain.control().retire(access, lease, key);
            }
            methods::DetachAction::CloseAbandonedOpen => {
                self.domain.control().close(access, lease);
            }
            methods::DetachAction::None => {}
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Core<'static, 'static, 'static, 'static, 'static, 0, Publish>>()
        == std::mem::size_of::<usize>()
);
const _: () = assert!(
    std::mem::align_of::<Core<'static, 'static, 'static, 'static, 'static, 0, Publish>>()
        == std::mem::align_of::<usize>()
);
