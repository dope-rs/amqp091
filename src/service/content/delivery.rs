use std::num;

use crate::message::content;
use crate::service::content::pending;

pub(crate) type Delivery<'c, 'b, 'd> = pending::Lease<'c, 'b, 'd, content::DeliveryText>;

impl<'c, 'b, 'd> Delivery<'c, 'b, 'd> {
    pub(crate) fn into_tag(self) -> num::NonZero<u64> {
        self.delivery_tag()
    }

    pub(crate) fn consumer_tag(&self) -> &str {
        self.delivery_str(content::DeliveryText::CONSUMER_TAG)
    }

    pub(crate) fn exchange(&self) -> &str {
        self.delivery_str(content::DeliveryText::EXCHANGE)
    }

    pub(crate) fn routing_key(&self) -> &str {
        self.delivery_str(content::DeliveryText::ROUTING_KEY)
    }
}
