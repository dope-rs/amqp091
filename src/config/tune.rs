use crate::config::Frame;

#[derive(Clone, Copy)]
pub struct Tune {
    pub(crate) channel_max: u16,
    pub(crate) frame: Frame,
    pub(crate) heartbeat: u16,
}

impl Default for Tune {
    fn default() -> Self {
        Self::new(2047, Frame::default(), 60)
    }
}

impl Tune {
    pub const fn new(channels: u16, frame: Frame, beats: u16) -> Self {
        Self {
            channel_max: channels,
            frame,
            heartbeat: beats,
        }
    }

    pub const fn channel_max(mut self, value: u16) -> Self {
        self.channel_max = value;
        self
    }

    pub const fn frame(mut self, value: Frame) -> Self {
        self.frame = value;
        self
    }

    pub const fn heartbeat(mut self, value: u16) -> Self {
        self.heartbeat = value;
        self
    }
}
