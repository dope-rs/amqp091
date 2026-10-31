use dope::core::driver::schedule::clock::timer;

use crate::config;

pub(in crate::service) struct Settings<'d> {
    pub(in crate::service::port) timer: timer::Timer<'d>,
    pub(in crate::service::port) config: config::Config,
}

impl<'d> Settings<'d> {
    pub(in crate::service) const fn timer(&self) -> timer::Timer<'d> {
        self.timer
    }
}
