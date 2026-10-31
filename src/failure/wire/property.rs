#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Flags { actual: u16, allowed: u16 },
    DeliveryMode { actual: u8 },
    Priority { actual: u8, max: u8 },
    Reserved { actual: usize },
    Trailing { offset: usize, remaining: usize },
}

const _: () = assert!(std::mem::size_of::<Fault>() == 3 * std::mem::size_of::<usize>());
