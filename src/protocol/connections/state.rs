use super::super::*;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::protocol) enum ConnPhase {
    #[default]
    NeedsHello,
    AwaitingStart,
    AwaitingTune,
    AwaitingOpenOk,
    Ready,
    Closed,
}

#[derive(Default)]
pub struct ConnState<'d> {
    pub(in crate::protocol) phase: ConnPhase,
    pub negotiated: Negotiated,
    pub(in crate::protocol) channels: Cell<*const Channels<'d>>,
    pub(in crate::protocol) pending_close: bool,
    pub(in crate::protocol) close_permanent: bool,
}

impl<'d> ConnState<'d> {
    pub(in crate::protocol) fn channels(&self) -> &Channels<'d> {
        let channels = self.channels.get();
        assert!(!channels.is_null());
        unsafe { &*channels }
    }

    pub(crate) fn with_channel_mut<R>(
        &self,
        id: u16,
        f: impl for<'a> FnOnce(&'a mut ChannelState<'d>) -> R,
    ) -> Option<R> {
        self.channels().with_channel_mut(id, f)
    }

    pub(in crate::protocol) fn for_each_channel_mut(&self, f: impl FnMut(&mut ChannelState<'d>)) {
        self.channels().for_each_mut(f);
    }

    pub fn is_ready(&self) -> bool {
        self.phase == ConnPhase::Ready
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.phase, ConnPhase::Closed)
    }
}

impl connector::Lifecycle for ConnState<'_> {
    fn wants_close(&self) -> Close {
        if !self.pending_close {
            Close::Keep
        } else if self.close_permanent {
            Close::Permanent
        } else {
            Close::Reconnect
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}
