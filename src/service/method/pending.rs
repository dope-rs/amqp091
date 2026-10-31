use o3::buffer;
use o3::buffer::hold::scope;

use crate::failure::{self, wire};
use crate::protocol::method::{self, Id, channel, conn};
use crate::protocol::wire::frame;
use crate::state::conn::phase::Phase;

use super::gates;

pub(super) enum Pending<'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    Connection(Connection<'input, 'd, R>),
    Channel(Channel<'input, 'd, R>),
}

pub(super) struct Connection<'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    kind: conn::Kind,
    arguments: scope::Deferred<'input, 'd, R>,
}

pub(super) struct Channel<'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    channel: frame::Channel,
    id: Id,
    arguments: scope::Deferred<'input, 'd, R>,
}

impl<'input, 'd, R> Pending<'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    pub(super) fn inspect(
        raw: u16,
        input: scope::Deferred<'input, 'd, R>,
    ) -> Result<Self, failure::Error> {
        let (bytes, args) = input
            .try_split::<{ Id::SIZE }>()
            .map_err(|cause| failure::Error::Protocol(wire::Fault::MethodSize(cause)))?;
        let id = Id::from_bytes(*bytes);
        if let Some(channel) = frame::Channel::new(raw) {
            return Ok(Self::Channel(Channel {
                channel,
                id,
                arguments: args,
            }));
        }
        if !id.is_conn() {
            return Err(failure::Error::Protocol(wire::Fault::ChannelZero));
        }
        Ok(Self::Connection(Connection {
            kind: conn::Kind::identify(id.method())?,
            arguments: args,
        }))
    }
}

impl<'input, 'd, R> Connection<'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    pub(super) fn admit(
        &self,
        budget: &buffer::resident::Budget<'d>,
        phase: Phase,
        closing: bool,
    ) -> Result<Option<conn::Event<'input, 'd>>, failure::Error> {
        if !gates::connection(self.kind, phase, closing)? {
            return Ok(None);
        }
        conn::Event::decode(&self.arguments, self.kind, budget).map(Some)
    }
}

impl<'input, 'd, R> Channel<'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    pub(super) fn admit<A>(
        &self,
        budget: &buffer::resident::Budget<'d>,
        admit: impl FnOnce(
            frame::Channel,
            Result<method::Kind, failure::Error>,
        ) -> Result<Option<(A, method::Kind)>, failure::Error>,
    ) -> Result<Option<(A, channel::Event<'input, 'd>)>, failure::Error> {
        let kind = if self.id.is_conn() {
            Err(failure::Error::Protocol(wire::Fault::ConnectionChannel {
                channel: self.channel.get(),
            }))
        } else {
            method::Kind::identify(self.id)
        };
        let Some((gate, kind)) = admit(self.channel, kind)? else {
            return Ok(None);
        };
        let event = channel::Event::decode(&self.arguments, kind, budget)?;
        Ok(Some((gate, event)))
    }
}
