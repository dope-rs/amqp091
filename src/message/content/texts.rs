use crate::protocol::wire::schema;

#[derive(Clone, Copy)]
pub(crate) enum ReturnText {}

impl ReturnText {
    pub(crate) const REPLY: schema::Field<Self, 0> = schema::Field::new();
    pub(crate) const EXCHANGE: schema::Field<Self, 1> = schema::Field::new();
    pub(crate) const ROUTING_KEY: schema::Field<Self, 2> = schema::Field::new();
}

#[derive(Clone, Copy)]
pub(crate) enum DeliveryText {}

impl DeliveryText {
    pub(crate) const CONSUMER_TAG: schema::Field<Self, 0> = schema::Field::new();
    pub(crate) const EXCHANGE: schema::Field<Self, 1> = schema::Field::new();
    pub(crate) const ROUTING_KEY: schema::Field<Self, 2> = schema::Field::new();
}
