/// AMQP method routing tuple.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct Route {
    channel: u16,
    class: u16,
    method: u16,
}

impl Route {
    pub(crate) const fn new(channel: u16, class: u16, method: u16) -> Self {
        Self {
            channel,
            class,
            method,
        }
    }

    pub(crate) const fn channel(self) -> u16 {
        self.channel
    }

    pub(crate) const fn class(self) -> u16 {
        self.class
    }

    pub(crate) const fn method(self) -> u16 {
        self.method
    }
}
