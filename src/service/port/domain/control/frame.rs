use o3::buffer::pool;

use crate::failure;
use crate::protocol::method::{self, basic, channel};
use crate::protocol::wire::Writer;
use crate::protocol::wire::text::short;
use crate::service::port::domain::egress::frame::Frame;
use crate::state::channel::methods::send;

impl Frame<(), Writer> {
    pub(super) fn write(
        self,
        channel: u16,
        command: send::Command<'_>,
    ) -> Result<pool::Cursor, failure::Error> {
        self.encode(|output| match command {
            send::Command::Cancel(tag) => {
                method::Encode::write(&basic::CancelRef { consumer_tag: tag }, output, channel)
            }
            send::Command::Close => method::Encode::write(
                &channel::CloseRef {
                    reply_code: 200,
                    reply_text: short::literal!(short::Short::try_new("open future dropped")),
                    class_id: 0,
                    method_id: 0,
                },
                output,
                channel,
            ),
        })
        .map(Frame::into_output)
    }
}
