use o3::buffer;
use o3::buffer::hold::scope;

use crate::failure;
use crate::protocol::method::conn::{self, Kind};
use crate::protocol::wire::text::fields;

pub(crate) enum Event<'input, 'd> {
    Start(conn::Start<'input>),
    Secure(conn::Secure),
    Tune(conn::Tune),
    Open(conn::OpenOk),
    Close(conn::Close<'d>),
    CloseOk(conn::CloseOk),
    Blocked(conn::Blocked<'d>),
    Unblocked(conn::Unblocked),
}

impl<'input, 'd> Event<'input, 'd> {
    pub(crate) fn decode<R>(
        args: &scope::Deferred<'input, 'd, R>,
        kind: Kind,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<Self, failure::Error>
    where
        R: buffer::Cursor + 'input,
    {
        match kind {
            Kind::Start => fields::Args::new(args)
                .decode(conn::Start::read)
                .map(Self::Start),
            Kind::Secure => fields::Args::new(args)
                .decode(conn::Secure::read)
                .map(Self::Secure),
            Kind::Tune => fields::Args::new(args)
                .decode(conn::Tune::read)
                .map(Self::Tune),
            Kind::Open => fields::Args::new(args)
                .decode(conn::OpenOk::read)
                .map(Self::Open),
            Kind::Close => conn::Close::retain(args, budget).map(Self::Close),
            Kind::CloseOk => fields::Args::new(args)
                .empty()
                .map(|()| Self::CloseOk(conn::CloseOk)),
            Kind::Blocked => conn::Blocked::retain(args, budget).map(Self::Blocked),
            Kind::Unblocked => fields::Args::new(args)
                .empty()
                .map(|()| Self::Unblocked(conn::Unblocked)),
        }
    }
}
