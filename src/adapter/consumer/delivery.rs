use std::num;

use crate::adapter::conn;
use crate::adapter::consumer::{Mode, Origin, Settle};
use crate::message::ack::{self, Disposition};
use crate::message::content::{Body, Properties};
use crate::service::content::delivery;

pub struct Delivery<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M: Mode = ack::Manual> {
    content: delivery::Delivery<'c, 'b, 'd>,
    source: M::Source<'o, 'r, 'c, 'b, 'd, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M: Mode>
    Delivery<'o, 'r, 'c, 'b, 'd, ID, M>
{
    pub(super) fn new(
        item: delivery::Delivery<'c, 'b, 'd>,
        origin: Origin<'o, 'r, 'c, 'b, 'd, ID>,
    ) -> Self {
        Self {
            content: item,
            source: M::bind(origin),
        }
    }

    pub fn delivery_tag(&self) -> num::NonZero<u64> {
        self.content.delivery_tag()
    }

    pub fn is_repeat(&self) -> bool {
        self.content.is_repeat()
    }

    pub fn consumer_tag(&self) -> &str {
        self.content.consumer_tag()
    }

    pub fn exchange(&self) -> &str {
        self.content.exchange()
    }

    pub fn routing_key(&self) -> &str {
        self.content.routing_key()
    }

    pub fn properties(&self) -> Properties<'_, 'd> {
        self.content.properties()
    }

    pub fn body(&self) -> Body<'_, 'b, 'd> {
        self.content.body()
    }

    /// Releases the payload lease without settling its delivery tag.
    pub fn release(self) {
        self.content.release();
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>
    Delivery<'o, 'r, 'c, 'b, 'd, ID, ack::Manual>
{
    pub fn epoch(&self) -> conn::Epoch<'d, ID> {
        self.source.epoch()
    }

    pub fn settle(self, kind: Disposition) -> Settle<'o, 'r, 'c, 'b, 'd, ID> {
        let Self { content, source } = self;
        let tag = content.into_tag();
        Settle::new(source, tag, kind)
    }
}

const _: () = {
    assert!(
        size_of::<Delivery<'static, 'static, 'static, 'static, 'static, 0, ack::Manual>>()
            == 5 * size_of::<usize>()
    );
    assert!(
        size_of::<Delivery<'static, 'static, 'static, 'static, 'static, 0, ack::Auto>>()
            == size_of::<usize>()
    );
};
