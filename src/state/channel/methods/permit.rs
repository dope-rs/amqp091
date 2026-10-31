use crate::state::channel::methods::{MethodKey, entry, flight};

#[must_use = "the reserved method must be committed with its frame"]
pub(crate) struct Permit<'a, 'c, 'b, 'd, const ID: u8> {
    key: MethodKey,
    state: &'a mut entry::Pending<'b, 'd>,
    flight: &'a mut flight::Flight<'c, 'b, 'd, ID>,
}

impl<'c, 'b, 'a, 'd, const ID: u8> Permit<'a, 'c, 'b, 'd, ID> {
    pub(super) const fn new(
        key: MethodKey,
        state: &'a mut entry::Pending<'b, 'd>,
        flight: &'a mut flight::Flight<'c, 'b, 'd, ID>,
    ) -> Self {
        Self { key, state, flight }
    }

    pub(crate) fn commit(self, response: flight::Prepared<'c, 'b, 'd, ID>) {
        self.flight.put(self.key, response);
        self.state.sent = true;
    }
}

const _: () =
    assert!(std::mem::size_of::<Permit<'_, '_, '_, '_, 0>>() == 3 * std::mem::size_of::<usize>());
