use std::{cell, ops};

use o3::buffer::pool;

use crate::protocol::wire::Writer;
use crate::service::port::domain::egress::frame::Frame;

pub(in crate::service::port::domain) struct Regular {
    pool: pool::Pool,
    free: cell::Cell<usize>,
}

impl Regular {
    pub(super) fn new(pool: pool::Pool) -> Self {
        let free = cell::Cell::new(pool.available());
        Self { pool, free }
    }

    pub(in crate::service::port::domain) fn acquire(&self) -> Option<Frame<(), Writer>> {
        let buffer = self.pool.cursor()?;
        self.free.set(self.pool.available());
        Some(Frame::new(Writer::new(buffer)))
    }

    pub(in crate::service::port::domain) fn is_available(&self) -> bool {
        self.pool.available() != 0
    }

    pub(in crate::service::port::domain) fn released(&self) -> ops::Range<usize> {
        let free = self.pool.available();
        self.free.replace(free)..free
    }
}
