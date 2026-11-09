use std::convert::Infallible;
use std::net::SocketAddr;
use std::{pin, time};

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
use dope::runtime::executor::{self, drive, session};
use dope_amqp091::service::{Logic, Port};
use dope_amqp091_fiber::conn::Connection;
use dope_fiber::abi::{self, Fiber};
use dope_fiber::task::done;

type App<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, const ID: u8, P, Q> = session::App<
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
    Q,
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
    Q = (),
> where
    P: settings::Profile + timing::Policy,
{
    app: App<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, ID, P, Q>,
    port: &'o Port<'r, 'c, 'b, 'd, ID>,
    deadline: time::Duration,
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
    Q,
> Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, ID, P, Q>
where
    P: settings::Profile + timing::Policy,
    Q: executor::Startup,
{
    pub(super) const fn new(
        app: App<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, ID, P, Q>,
        port: &'o Port<'r, 'c, 'b, 'd, ID>,
        deadline: time::Duration,
    ) -> Self {
        Self {
            app,
            port,
            deadline,
        }
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

    pub fn block_on<F>(&mut self, fiber: F) -> Result<F::Output, drive::Error>
    where
        F: Fiber<'d>,
    {
        self.app.drive_for(self.deadline, done::Once::new(fiber))
    }

    pub fn until_shutdown<F>(&mut self, fiber: F) -> Result<Option<F::Output>, drive::Error>
    where
        F: Fiber<'d>,
    {
        let mut output = None;
        let target = &mut output;
        let result = self.block_on(dope_fiber::fiber!('d => async move {
            *target = Some(fiber.await);
            abi::Pending::<Infallible>::new().await
        }));
        match result {
            Err(drive::Error::Shutdown) => Ok(output),
            Err(error) => Err(error),
            Ok(never) => match never {},
        }
    }
}
