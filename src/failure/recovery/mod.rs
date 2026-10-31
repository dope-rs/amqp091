mod channel;
mod class;
mod fault;
mod policy;
mod session;
mod terminal;

pub(crate) use channel::Channel;
pub(crate) use class::Class;
pub(crate) use fault::Fault;
pub(crate) use policy::Policy;
pub(crate) use session::Session;
pub(crate) use terminal::Terminal;

use crate::failure;
use crate::protocol::wire::text::resident;

pub(crate) enum Connection {}

impl Connection {
    const ERROR: u16 = 302;
    const FORCED: u16 = 320;
}

impl Policy for Connection {
    fn classify<'b, 'd>(
        failure: Fault<'b, 'd, Self>,
    ) -> (Class, failure::Error<resident::Short<'b, 'd>>) {
        let class = match failure.error().server_code() {
            Some(Self::ERROR | Self::FORCED) => Class::Recoverable,
            _ => Class::Fatal,
        };
        (class, failure.into_error())
    }
}
