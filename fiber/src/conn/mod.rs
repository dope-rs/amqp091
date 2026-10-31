use std::pin;

mod channels;
mod control;
mod observe;
mod retired;

pub use channels::Channels;
pub use control::Control;
use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::clock;
use dope::manifold::service::bindings;
use dope::runtime::executor::session;
use dope_amqp091::adapter::conn::{Epoch, Ready};
use dope_amqp091::{adapter, service};
pub use observe::Observe;
pub use retired::Retired;

use crate::runtime;

/// Topology-ready lifetime of one physical transport generation.
///
/// Invariant in the driver lifetime and branded to one route, the lease pairs
/// the exact readiness proof with its diagnostic generation and absolute deadline.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Lease<'d, const ID: u8> {
    ready: Ready<'d, ID>,
    generation: u64,
    expires_at: clock::Deadline<'d>,
}

impl<'d, const ID: u8> Lease<'d, ID> {
    pub const fn ready(self) -> Ready<'d, ID> {
        self.ready
    }

    pub const fn epoch(self) -> Epoch<'d, ID> {
        self.ready.epoch()
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn expires_at(self) -> clock::Deadline<'d> {
        self.expires_at
    }
}

/// Application-scoped root capability for one AMQP route.
///
/// Carries identity and lifetime; operations are available through `observe`,
/// `control`, and `channels`.
pub struct Connection<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
}

impl<B: ?Sized, const ID: u8> Clone for Connection<'_, '_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: ?Sized, const ID: u8> Copy for Connection<'_, '_, '_, '_, '_, '_, B, ID> {}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Connection<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub const fn observe(self) -> Observe<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        Observe::new(self.handle)
    }

    pub const fn control(self) -> Control<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        Control::new(self.handle)
    }

    pub const fn channels(self) -> Channels<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        Channels::new(self.handle)
    }
}

const _: () = assert!(
    std::mem::size_of::<Lease<'static, 0>>()
        == std::mem::size_of::<(Ready<'static, 0>, u64, clock::Deadline<'static>)>()
);
const _: () = assert!(
    std::mem::size_of::<Connection<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == std::mem::size_of::<usize>()
);

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, const ID: u8, F>
    Connection<
        'app,
        'o,
        'r,
        'c,
        'b,
        'd,
        bindings::Bound<
            'o,
            'd,
            service::Port<'r, 'c, 'b, 'd, ID>,
            service::Logic<'o, 'd, ID>,
            KeyTag<ID>,
            1,
            F,
        >,
        ID,
    >
{
    /// Issues a connection from its installed manifold session.
    pub fn new<'life: 'app, D, Q, Plan>(
        access: &mut session::Access<'app, 'life, 'd, D, Q, Plan>,
        select: impl for<'borrow> FnOnce(
            pin::Pin<&'borrow D>,
        ) -> pin::Pin<
            &'borrow bindings::Bound<
                'o,
                'd,
                service::Port<'r, 'c, 'b, 'd, ID>,
                service::Logic<'o, 'd, ID>,
                KeyTag<ID>,
                1,
                F,
            >,
        >,
    ) -> Self
    where
        'd: 'life,
    {
        access.client_with(select, |context| {
            let (edge, scope) = context.into_parts();
            Self {
                handle: runtime::Handle::new(adapter::Adapter::new(edge.get_ref().root()), scope),
            }
        })
    }
}
