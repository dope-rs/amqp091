use std::time;

use o3::base::time::Positive;

/// Static resource, retry, lifetime, and test-deadline policy for one AMQP session.
pub struct Policy {
    pub(super) connections: usize,
    pub(super) waiters: usize,
    pub(super) backoff_base: time::Duration,
    pub(super) backoff_maximum: time::Duration,
    pub(super) deadline: time::Duration,
    pub(super) transport_max_age: Option<Positive>,
}

impl Policy {
    /// Uses the standard retry window with explicit capacities and test deadline.
    pub const fn new(conn: usize, waiters: usize, deadline: time::Duration) -> Self {
        Self {
            connections: conn,
            waiters,
            backoff_base: time::Duration::from_millis(500),
            backoff_maximum: time::Duration::from_secs(30),
            deadline,
            transport_max_age: None,
        }
    }

    /// Replaces the reconnect backoff window.
    pub(crate) const fn backoff(mut self, base: time::Duration, maximum: time::Duration) -> Self {
        self.backoff_base = base;
        self.backoff_maximum = maximum;
        self
    }

    /// Retires transports after the supplied maximum age.
    pub(crate) const fn retire_after(mut self, maximum: Positive) -> Self {
        self.transport_max_age = Some(maximum);
        self
    }
}
