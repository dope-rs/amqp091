use crate::state::channel::methods::entry;
use crate::state::channel::methods::send::Effect;
use crate::state::channel::methods::send::dispatch::Dispatch;

/// Exclusive claim on the current control operation.
#[repr(transparent)]
pub(crate) struct Pending<'a, 'b, 'd> {
    state: &'a mut entry::Pending<'b, 'd>,
}

impl<'b, 'a, 'd> Pending<'a, 'b, 'd> {
    pub(in crate::state::channel::methods) const fn new(
        state: &'a mut entry::Pending<'b, 'd>,
    ) -> Self {
        Self { state }
    }

    pub(crate) fn preview<R>(
        self,
        send: impl for<'s> FnOnce(Dispatch<'s, 'b, 'd>) -> Option<R>,
    ) -> Option<R> {
        match &self.state.outbound {
            entry::Outbound::Consumer(key) => send(Dispatch::Consumer(*key))?,
            entry::Outbound::Orphan(tag) => send(Dispatch::Orphan(tag))?,
            entry::Outbound::Close => send(Dispatch::Close)?,
            entry::Outbound::None => return None,
        }
        .into()
    }

    pub(crate) fn commit(self) -> Effect {
        self.state.sent = true;
        if matches!(self.state.outbound, entry::Outbound::Close) {
            Effect::Closing
        } else {
            Effect::Sent
        }
    }
}

const _: () = assert!(std::mem::size_of::<Pending<'_, '_, '_>>() == std::mem::size_of::<usize>());
