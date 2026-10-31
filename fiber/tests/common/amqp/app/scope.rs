use std::net::SocketAddr;
use std::pin;

use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::clock::timer;
use dope::core::driver::settings;
use dope::manifold::app::Host;
use dope::manifold::app::env::Bundle;
use dope::manifold::network::client;
use dope::manifold::service::bindings::failures;
use dope::manifold::service::{bindings, sync};
use dope::manifold::timing;
use dope::net::tcp::Tcp;
use dope::net::wire::Identity;
use dope::runtime::executor::{drive, session};
use dope_amqp091::service::{Logic, Port};
use dope_amqp091_fiber::conn::Connection;
use dope_fiber::BlockOn as _;
use dope_fiber::abi::Fiber;
use dope_test::fibers;

type App<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, const ID: u8, P> = session::App<
    'app,
    'life,
    'd,
    Host<
        client::Pool<
            'q,
            'h,
            'p,
            'd,
            'd,
            KeyTag<ID>,
            1,
            bindings::Bound<
                'o,
                'd,
                Port<'r, 'c, 'b, 'd, ID>,
                Logic<'o, 'd, ID>,
                KeyTag<ID>,
                1,
                failures::External<()>,
            >,
            sync::Preserve,
            SocketAddr,
            Bundle<Tcp, Identity, P>,
            1,
        >,
    >,
>;

/// One installed AMQP application whose handles cannot outlive its driver domain.
pub struct Scope<
    'app,
    'life: 'app,
    'q: 'app,
    'h: 'app,
    'p: 'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'life + 'p + 'b,
    const ID: u8,
    P,
> where
    P: settings::Profile + timing::Policy,
{
    app: App<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, ID, P>,
    port: &'o Port<'r, 'c, 'b, 'd, ID>,
    fibers: fibers::Harness,
}

impl<
    'app,
    'life: 'app,
    'q: 'app,
    'h: 'app,
    'p: 'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'life + 'p + 'b,
    const ID: u8,
    P,
> Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, ID, P>
where
    P: settings::Profile + timing::Policy,
{
    pub(super) const fn new(
        app: App<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, ID, P>,
        port: &'o Port<'r, 'c, 'b, 'd, ID>,
        fibers: fibers::Harness,
    ) -> Self {
        Self { app, port, fibers }
    }

    pub fn connection(
        &mut self,
    ) -> Connection<
        'app,
        'o,
        'r,
        'c,
        'b,
        'd,
        bindings::Bound<
            'o,
            'd,
            Port<'r, 'c, 'b, 'd, ID>,
            Logic<'o, 'd, ID>,
            KeyTag<ID>,
            1,
            failures::External<()>,
        >,
        ID,
    > {
        Connection::new(&mut self.app, |host| {
            let link = host.manifold();
            pin::Pin::new(link.get_ref().session())
        })
    }

    pub const fn port(&self) -> &'o Port<'r, 'c, 'b, 'd, ID> {
        self.port
    }

    pub fn timer(&self) -> timer::Timer<'d> {
        self.port.timer()
    }

    pub fn block_on<F>(&mut self, fiber: F) -> Result<F::Output, fibers::Run>
    where
        F: Fiber<'d>,
    {
        self.fibers.try_run(&mut self.app, fiber)
    }

    pub fn drive<F>(&mut self, fiber: F) -> Result<F::Output, drive::Error>
    where
        F: Fiber<'d>,
    {
        self.app.block_on(fiber)
    }
}
