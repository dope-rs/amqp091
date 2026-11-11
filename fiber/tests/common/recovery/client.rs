use std::{net, time};

use dope::core::driver::life::recovery;
use dope::manifold::timing;
use dope_amqp091::config;

use crate::common::amqp::app::{Endpoint, Policy, Scope, error};

pub(crate) const POLICY: Policy = Policy::new(1, 16, time::Duration::from_secs(10)).backoff(
    time::Duration::from_millis(10),
    time::Duration::from_secs(1),
);

pub fn with(
    address: net::SocketAddr,
    test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
        &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, timing::Throughput>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<(), error::Error<'owner, ()>>),
) {
    let config = match config::Config::new("guest", "guest") {
        Ok(config) => config,
        Err(error) => return finish(Err(error::Error::Config(error))),
    };
    with_config(address, config, test, recover, finish)
}

pub fn with_config(
    address: net::SocketAddr,
    config: config::Config,
    test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
        &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, timing::Throughput>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<(), error::Error<'owner, ()>>),
) {
    Endpoint::new(address, config)
        .try_check::<timing::Throughput, _, _>(POLICY, test, recover, finish)
}

pub fn complete<Q>(result: Result<(), error::Error<'_, (), Q>>) {
    result
        .as_ref()
        .map_err(|error| error.kind())
        .expect("endpoint completion");
}
