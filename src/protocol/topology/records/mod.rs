pub(crate) mod consumer;
pub(crate) mod exchange;
pub(crate) mod queue;

#[derive(Clone, Copy)]
pub(crate) struct Qos {
    pub(crate) prefetch_size: u32,
    pub(crate) prefetch_count: u16,
    pub(crate) global: bool,
}
