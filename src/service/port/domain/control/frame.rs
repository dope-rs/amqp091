use o3::buffer::pool;

use crate::failure::{self, wire};
use crate::protocol::method::{self, basic, channel};
use crate::protocol::wire::Writer;
use crate::protocol::wire::text::short;
use crate::state::channel::methods::send;

#[repr(transparent)]
pub(in crate::service::port::domain) struct Frame {
    output: Writer,
}

impl Frame {
    pub(in crate::service::port::domain) const fn new(output: Writer) -> Self {
        Self { output }
    }

    pub(super) fn write(
        mut self,
        channel: u16,
        command: send::Command<'_>,
    ) -> Result<pool::Cursor, failure::Error> {
        match command {
            send::Command::Cancel(tag) => method::Encode::write(
                &basic::CancelRef { consumer_tag: tag },
                &mut self.output,
                channel,
            ),
            send::Command::Close => method::Encode::write(
                &channel::CloseRef {
                    reply_code: 200,
                    reply_text: short::literal!(short::Short::try_new("open future dropped")),
                    class_id: 0,
                    method_id: 0,
                },
                &mut self.output,
                channel,
            ),
        }
        let output = self
            .output
            .try_finish()
            .map_err(|error| failure::Error::Capacity(wire::Capacity::Outbound(error)))?;
        Ok(output)
    }
}

const _: () = assert!(std::mem::size_of::<Frame>() == std::mem::size_of::<Writer>());
