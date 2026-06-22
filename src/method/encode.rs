use super::spec::Method;
use crate::wire::{FrameEncoder, FrameType, Put, Sink};

pub trait ClientMethod: Method {
    fn write_args<S: Sink>(&self, out: &mut S);

    fn write_frame<S: Sink>(&self, out: &mut S, channel: u16) {
        FrameEncoder::write(out, FrameType::METHOD, channel, |s| {
            s.put_u16(Self::CLASS_ID);
            s.put_u16(Self::METHOD_ID);
            self.write_args(s);
        });
    }
}
