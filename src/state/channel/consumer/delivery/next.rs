use crate::service::content::delivery::Delivery;
use crate::state::channel::Freed;

#[must_use]
pub(crate) enum Next<'c, 'b, 'd> {
    Unavailable,
    Pending,
    Closed,
    Delivery {
        delivery: Delivery<'c, 'b, 'd>,
        freed: Freed,
    },
}
