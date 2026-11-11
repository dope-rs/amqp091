use std::net::SocketAddr;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope::manifold::timing;
use dope_amqp091::config;

use crate::common::amqp::app::{Endpoint, Policy, Scope, error};
use crate::common::amqp::wire::{self, Method};
use crate::common::amqp::{Error, Tune};
pub(crate) const FRAMES: usize = 64;
pub(crate) const POLICY: Policy = Policy::new(1, 16, Duration::from_secs(10));
pub const TUNE: Tune = Tune::new(1, 4096, 0);

pub fn with_client(
    address: SocketAddr,
    test: impl for<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd> FnOnce(
        &mut Scope<'app, 'life, 'q, 'h, 'p, 'o, 'r, 'c, 'b, 'd, 0, timing::Throughput>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<(), error::Error<'owner, ()>>),
) {
    let channel = config::Channel::default().confirms(1);
    let capacity = config::Capacity::default()
        .channel(channel)
        .egress(config::Egress::new(FRAMES, 4096));
    let config = match config::Config::new("guest", "guest") {
        Ok(config) => config.capacity(capacity),
        Err(error) => return finish(Err(error::Error::Config(error))),
    };
    with_config(address, config, test, recover, finish)
}

pub fn with_config(
    address: SocketAddr,
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

pub fn routing_key(frame: &Method) -> Result<&str, Error> {
    let (_, offset) = wire::short(&frame.1, 6)?;
    wire::short(&frame.1, offset).map(|(key, _)| key)
}

pub mod blocked;

pub mod commit;
