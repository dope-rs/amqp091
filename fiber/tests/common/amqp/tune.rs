/// Server limits offered by Connection.Tune.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Tune {
    channels: u16,
    frame: u32,
    interval: u16,
}

impl Tune {
    pub const fn new(channels: u16, frame: u32, interval: u16) -> Self {
        Self {
            channels,
            frame,
            interval,
        }
    }

    pub(crate) const fn parts(self) -> (u16, u32, u16) {
        (self.channels, self.frame, self.interval)
    }
}
