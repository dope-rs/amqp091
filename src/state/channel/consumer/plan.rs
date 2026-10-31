use o3::depot::arena::hash;

use crate::failure::{self, wire};

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    pub(super) resident: hash::Plan,
}

impl Plan {
    pub(crate) fn new(capacity: usize) -> Result<Self, failure::Error> {
        let resident = hash::Plan::new(capacity)
            .map_err(|error| failure::Error::InvalidConfig(wire::Config::ConsumerHash { error }))?;
        Ok(Self { resident })
    }
}
