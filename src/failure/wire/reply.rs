#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reply {
    Frame,
    Command,
    Unexpected,
    Channel,
    Syntax,
}
