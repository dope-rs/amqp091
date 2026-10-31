use crate::protocol::wire::text::short;

mod dispatch;
mod next;
mod pending;
mod prepared;

pub(crate) use dispatch::Dispatch;
pub(crate) use next::Next;
pub(crate) use pending::Pending;
pub(crate) use prepared::Prepared;

pub(crate) enum Command<'a> {
    Cancel(short::Short<'a>),
    Close,
}

pub(crate) enum Effect {
    Sent,
    Closing,
}
