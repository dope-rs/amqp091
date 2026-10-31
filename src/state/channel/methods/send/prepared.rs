use crate::state::channel;
use crate::state::channel::methods::send;

/// Exclusive proof that the current control operation is ready to be sent.
#[must_use = "the prepared control operation must be committed with its frame"]
pub(crate) struct Prepared<'a, 'b, 'd> {
    phase: &'a mut channel::Phase,
    pending: send::Pending<'a, 'b, 'd>,
}

impl<'b, 'a, 'd> Prepared<'a, 'b, 'd> {
    pub(in crate::state::channel) const fn new(
        phase: &'a mut channel::Phase,
        pending: send::Pending<'a, 'b, 'd>,
    ) -> Self {
        Self { phase, pending }
    }

    pub(crate) fn commit(self) -> send::Effect {
        let effect = self.pending.commit();
        if matches!(&effect, send::Effect::Closing) {
            *self.phase = channel::Phase::Closing;
        }
        effect
    }
}

const _: () = assert!(
    std::mem::size_of::<Prepared<'_, '_, '_>>() == std::mem::size_of::<(&mut (), &mut ())>()
);
