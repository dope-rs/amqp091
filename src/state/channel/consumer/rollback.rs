use crate::state::channel::consumer;

pub(crate) enum Rollback {
    New(consumer::Key),
    Replay(consumer::Key),
}
