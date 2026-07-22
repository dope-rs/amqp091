use super::super::*;

#[derive(Clone, Copy)]
pub(in crate::protocol) struct Limits {
    pub(in crate::protocol) methods: usize,
    pub(in crate::protocol) confirms: usize,
    pub(in crate::protocol) consumers: usize,
    pub(in crate::protocol) deliveries: usize,
    pub(in crate::protocol) returns: usize,
    pub(in crate::protocol) body: usize,
}

impl From<&Config> for Limits {
    fn from(config: &Config) -> Self {
        let ChannelCapacity {
            methods,
            confirms,
            consumers,
            deliveries,
            returns,
        } = config.per_channel;
        Self {
            methods,
            confirms,
            consumers,
            deliveries,
            returns,
            body: config.max_body_size,
        }
    }
}
