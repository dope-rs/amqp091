use crate::protocol::wire::text::short;

mod next;
mod pending;

pub(in crate::state::channel) use super::entry::Outbound;
pub(crate) use next::Next;
pub(crate) use pending::Pending;

pub(crate) enum Command<'a> {
    Cancel(short::Short<'a>),
    Close,
}

pub(crate) enum Effect {
    Sent,
    Closing,
}
