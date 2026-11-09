use std::num;

use crate::adapter::publish::{self, limit};
use crate::failure;
use crate::message::property;
use crate::protocol::wire::text::short;

/// Immutable publish input. Each borrow remains independent until its exact
/// wire stage commits.
pub struct Request<'exchange, 'key, 'properties, 'body> {
    pub exchange: &'exchange str,
    pub routing_key: &'key str,
    pub mandatory: bool,
    pub properties: &'properties property::Properties,
    pub body: &'body [u8],
}

pub(super) struct Checked<'exchange, 'key, 'properties, 'body> {
    pub(super) exchange: short::Name<'exchange>,
    pub(super) key: short::Short<'key>,
    pub(super) mandatory: bool,
    pub(super) properties: &'properties property::Properties,
    pub(super) body: &'body [u8],
    max: num::NonZero<u32>,
}

impl<'exchange, 'key, 'properties, 'body> Request<'exchange, 'key, 'properties, 'body> {
    pub(super) fn check(
        self,
        limit: limit::Limit,
    ) -> Result<Checked<'exchange, 'key, 'properties, 'body>, failure::Error> {
        let exchange = short::Short::resource(self.exchange, short::Resource::Exchange)?;
        let key = short::Short::new(self.routing_key)?;
        let method = publish::METHOD_FIXED_SIZE as u64
            + exchange.value().len() as u64
            + u64::from(key.len());
        let header = publish::FRAME_OVERHEAD as u64 + self.properties.encoded_len();
        validate(method, limit.frame())?;
        validate(header, limit.frame())?;
        Ok(Checked {
            exchange,
            key,
            mandatory: self.mandatory,
            properties: self.properties,
            body: self.body,
            max: limit.body(),
        })
    }
}

impl Checked<'_, '_, '_, '_> {
    pub(super) const fn max(&self) -> num::NonZero<u32> {
        self.max
    }
}

fn validate(size: u64, limit: usize) -> Result<(), failure::Error> {
    if size <= limit as u64 {
        return Ok(());
    }
    Err(failure::Error::FrameTooLarge {
        size,
        max: limit as u32,
    })
}

const _: () = {
    use std::mem;

    assert!(
        mem::size_of::<Request<'static, 'static, 'static, 'static>>()
            == 8 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Checked<'static, 'static, 'static, 'static>>()
            == 8 * mem::size_of::<usize>()
    );
};
