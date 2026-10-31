use o3::buffer::pool;

use crate::protocol::wire::Writer;
use crate::service::port::domain::control::frame::Frame;

pub(super) mod frame;
mod regular;

pub(in crate::service::port) struct Egress {
    pub(super) regular: regular::Regular,
    control: pool::Pool,
}

impl Egress {
    pub(in crate::service::port) fn new(regular: pool::Pool, control: pool::Pool) -> Self {
        Self {
            regular: regular::Regular::new(regular),
            control,
        }
    }

    pub(in crate::service::port::domain) fn control(&self) -> Option<Frame> {
        let buffer = self.control.cursor()?;
        Some(Frame::new(Writer::new(buffer)))
    }
}
