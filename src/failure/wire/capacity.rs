use o3::depot::capacity;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capacity {
    ChannelGeneration,
    Consumer,
    ContentProperty {
        attempted: usize,
        capacity: usize,
    },
    BodyFragments {
        capacity: capacity::Count,
    },
    BodyBudget(o3::mem::credit::Error),
    Delivery,
    InboundContent {
        attempted: usize,
        capacity: usize,
    },
    Method,
    MethodReplay,
    OutboundBuffer,
    Outbound(consume::Error),
    OutboundStaging,
    ProtocolHeader,
    PublishGeneration,
    ReplaySequence {
        current: u64,
        max: u64,
    },
    PublishConfirm {
        used: usize,
        max: capacity::Count,
    },
    ConfirmSlot {
        next: u64,
        occupied: u64,
        index: u32,
    },
    ConfirmGeneration {
        current: u64,
        tag: u64,
    },
    DeliveryTag {
        current: u64,
        max: u64,
    },
    Return,
    TerminalCause,
    TopologyIdentity,
    TopologyMetadata,
}

const _: () = assert!(std::mem::size_of::<Capacity>() == 4 * std::mem::size_of::<usize>());
use o3::buffer::write::consume;
