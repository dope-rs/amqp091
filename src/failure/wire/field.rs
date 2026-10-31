use crate::protocol::wire::reader;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Read(reader::Fault),
    Span {
        start: usize,
        len: usize,
        max: usize,
    },
    Text {
        start: usize,
        max: usize,
    },
    Tag {
        offset: usize,
        actual: u8,
    },
    Nodes {
        actual: usize,
        max: usize,
    },
    Depth {
        actual: usize,
        max: usize,
    },
    Trailing {
        offset: usize,
        remaining: usize,
    },
    Range {
        start: usize,
        end: usize,
        max: usize,
    },
}

const _: () = assert!(std::mem::size_of::<Fault>() == 6 * std::mem::size_of::<usize>());
