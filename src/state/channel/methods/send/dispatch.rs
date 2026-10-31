use crate::protocol::wire::text::resident;
use crate::state::channel::consumer;

pub(crate) enum Dispatch<'a, 'b, 'd> {
    Consumer(consumer::Key),
    Orphan(&'a resident::Short<'b, 'd>),
    Close,
}
