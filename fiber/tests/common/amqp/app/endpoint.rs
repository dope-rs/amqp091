use std::ops::ControlFlow;
use std::{env, net};

use dope::core::driver::life::creation::Attempt;
use dope::core::driver::life::recovery;
use dope::core::driver::settings;
use dope::core::driver::settings::claim::Needs;
use dope::core::io::Reclaimed;
use dope::manifold::app::Host;
use dope::manifold::app::env::Bundle;
use dope::manifold::service::{health, sync};
use dope::manifold::{service, timing};
use dope::net::tcp::Tcp;
use dope::net::wire::{Identity, domain};
use dope::runtime::executor;
use dope::runtime::executor::session;
use dope_amqp091::config;
use dope_amqp091::service::{Port, Spec, app};

use crate::common::amqp::app::error::Error;
use crate::common::amqp::app::{Policy, Scope, vars};

/// Address and protocol configuration for one AMQP integration-test endpoint.
pub struct Endpoint<Q = ()> {
    address: net::SocketAddr,
    config: config::Config,
    shutdown: Q,
}

impl Endpoint {
    pub const fn new(address: net::SocketAddr, config: config::Config) -> Self {
        Self {
            address,
            config,
            shutdown: (),
        }
    }

    pub fn from_vars(values: [Result<String, env::VarError>; 5]) -> Result<Self, vars::Error> {
        use vars::{Error, Key};

        let [host, port, user, password, vhost] = values;
        let host = Key::Host.resolve(host)?;
        let port = Key::Port.resolve(port)?;
        let port = port
            .parse()
            .map_err(|cause| Error::Port { value: port, cause })?;
        let host = host
            .parse()
            .map_err(|cause| Error::Host { value: host, cause })?;
        let user = Key::User.resolve(user)?;
        let password = Key::Password.resolve(password)?;
        let vhost = Key::Vhost.resolve(vhost)?;
        let config = config::Config::new(user, password)
            .map_err(Error::Config)?
            .vhost(vhost)
            .map_err(Error::Config)?;
        Ok(Self::new(net::SocketAddr::new(host, port), config))
    }

    pub fn shutdown<Q>(self, source: Q) -> Endpoint<Q> {
        Endpoint {
            address: self.address,
            config: self.config,
            shutdown: source,
        }
    }
}

impl<Q: executor::Build> Endpoint<Q> {
    pub fn tune(mut self, tune: config::Tune) -> Self {
        self.config = self.config.tune(tune);
        self
    }

    pub(crate) fn try_check<P, Done>(
        self,
        policy: Policy,
        run: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
            &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, P, Q::Shutdown>,
        ),
        recover: impl recovery::Recover,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner, (), Q::Shutdown>>) -> Done,
    ) -> Done
    where
        P: settings::Profile + timing::Policy,
    {
        let mut reports = Vec::new();
        let result = self.enter::<P, _, _>(
            policy,
            run,
            |event| reports.push(event),
            recover,
            |result| match result {
                Ok(()) => ControlFlow::Continue(finish),
                Err(error) => ControlFlow::Break(finish(Err(error))),
            },
        );
        match result {
            ControlFlow::Break(done) => done,
            ControlFlow::Continue(finish) => {
                if matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]) {
                    finish(Ok(()))
                } else {
                    finish(Err(Error::Reports(reports)))
                }
            }
        }
    }

    pub fn enter<P, R, Done>(
        self,
        policy: Policy,
        test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
            &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, P, Q::Shutdown>,
        ) -> R,
        report: impl FnMut(Reclaimed),
        recover: impl recovery::Recover,
        finish: impl for<'owner> FnOnce(Result<R, Error<'owner, R, Q::Shutdown>>) -> Done,
    ) -> Done
    where
        P: settings::Profile + timing::Policy,
    {
        let prepared = (|| {
            let spec = Spec::try_new(self.config, policy.connections, policy.waiters)
                .map_err(Error::Config)?;
            let spec = match policy.transport_max_age {
                Some(age) => spec.with_age(age),
                None => spec,
            };
            let snapshot = service::Snapshot::from_one(
                service::Revision::new(1),
                service::Endpoint::new(self.address, self.address),
            );
            let plan = app::Plan::try_for(&spec, policy.backoff_base, policy.backoff_maximum)
                .map_err(Error::Plan)?;
            let inputs = (spec, plan);
            let config = Needs::from_tree(&inputs)
                .and_then(|needs| needs.resolve::<P>())
                .map_err(Error::Claim)?;
            let (spec, plan) = inputs;
            Ok((spec, plan, snapshot, config))
        })();
        let (spec, plan, snapshot, config) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return finish(Err(error)),
        };
        let mut creation = Attempt::new();
        let executor = match self.shutdown.try_build(&mut creation, config) {
            Ok(executor) => executor,
            Err(error) => return finish(Err(Error::Executor(error))),
        };
        let mut outcome = session::Completion::new();
        executor.run(|active| {
            let result = active
                .enter_with(
                    |mut runtime| {
                        Port::try_with(
                            spec,
                            &mut runtime,
                            session::Scope::driver,
                            |port, runtime| {
                                let setup = port.bind(());
                                let backoff = runtime.hash_state(health::Domain::DEFAULT);
                                plan.into_inner()
                            .bind(domain::Local::new(Identity), backoff)
                            .install::<_, _, sync::Preserve, _, Bundle<Tcp, Identity, P>, 1, _, _>(
                                setup,
                                snapshot,
                                runtime,
                                session::Scope::driver,
                                |host, runtime| {
                                    runtime.with_app(&mut outcome, Host::new(host), |app| {
                                        test(&mut Scope::new(
                                            app,
                                            &port,
                                            policy.deadline,
                                        ))
                                    })
                                },
                            )
                            },
                        )
                    },
                    recovery::Policy::new(report, recover),
                )
                .map_err(Error::Domain)
                .and_then(|result| result.map_err(Error::Storage))
                .and_then(|result| result.map_err(Error::Install))
                .and_then(|result| result.map_err(Error::App));
            finish(result)
        })
    }
}
