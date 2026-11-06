#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    #[default]
    NeedsHello = 1,
    Start,
    Tune,
    Open,
    Ready,
}

const _: () = assert!(std::mem::size_of::<Phase>() == std::mem::size_of::<u8>());
