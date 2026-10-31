use crate::failure::terminal;

pub(crate) enum Event<'a, 'c, 'b, 'd> {
    TransportEnded {
        cause: &'a terminal::Lease<'c, 'b, 'd>,
    },
    Reopened,
    ChannelClosed,
}
