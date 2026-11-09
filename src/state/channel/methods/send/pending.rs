use crate::state::channel::methods::entry;
use crate::state::channel::methods::send::Effect;

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

    pub(in crate::state::channel) fn outbound(self) -> &'a entry::Outbound<'b, 'd> {
        &self.state.outbound
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
