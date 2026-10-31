#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Short,
    Queue,
    Exchange,
    VirtualHost,
    BrokerQueue,
    BrokerConsumer,
    Long,
    FieldArray,
    FieldTable,
}
