use std::time;

use dope::core::driver::route::KeyTag;
use dope::core::driver::settings::{self, claim, errors};
use dope::manifold::network::client::topology::plan;
use dope::manifold::service::health;

use crate::service;

/// AMQP-validated connector plan derived from its matching storage plan.
#[repr(transparent)]
pub struct Plan<const ID: u8>(plan::Plan<KeyTag<ID>, 1, plan::Standard, plan::External>);

#[derive(Debug)]
pub enum Error {
    Backoff(health::Error),
    Claim(errors::Claim),
}

impl<const ID: u8> Plan<ID> {
    /// Derives a connector plan without detaching it from AMQP storage policy.
    pub fn try_for(
        source: &service::Spec<ID>,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<Self, Error> {
        let mut spec = plan::Spec::new(base, max)
            .map_err(Error::Backoff)?
            .with_egress(source.plan.egress.retained);
        if let Some(age) = source.max_age {
            spec = spec.with_ttl(age);
        }
        plan::Plan::<KeyTag<ID>, 1, plan::Standard, plan::External>::new(
            source.plan.transport.staging.capacity().count().get(),
            spec,
        )
        .map(Self)
        .map_err(Error::Claim)
    }

    /// Releases the generic topology plan after AMQP validation.
    pub fn into_inner(self) -> plan::Plan<KeyTag<ID>, 1, plan::Standard, plan::External> {
        self.0
    }
}

impl<const ID: u8> settings::Resource for Plan<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.0.include_in(needs)
    }
}
