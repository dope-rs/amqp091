use std::num;

use publish::{confirms, streams};

use crate::failure;
use crate::state::channel::{self, consumer, methods, publish};

pub(crate) mod lanes;

pub(crate) enum Outcome {
    Reservation,
    Method(Result<methods::DetachResult, failure::Error>),
    Consumer(consumer::Key),
    Confirm,
    Publish,
}

pub(crate) struct Access<'a, 'c, 'b, 'd, const ID: u8> {
    pub(super) control: &'a mut channel::Control,
    pub(super) methods: &'a mut methods::Methods<'c, 'b, 'd, ID>,
    pub(super) consumers: &'a mut consumer::Set<'b, 'd, ID>,
    pub(super) confirm: &'a mut confirms::Ledger<'c, 'b, 'd>,
    pub(super) publishing: &'a mut streams::Flight,
}

impl<'c, 'b, 'd, const ID: u8> Access<'_, 'c, 'b, 'd, ID> {
    pub(crate) fn reap(&mut self, kind: lanes::Kind, id: num::NonZero<u64>) -> Option<Outcome> {
        match kind {
            lanes::Kind::Reservation => Some(Outcome::Reservation),
            lanes::Kind::Method => Some(Outcome::Method(self.methods.reap(
                &mut self.control.phase,
                self.publishing.is_active(),
                id,
            ))),
            lanes::Kind::Consumer => self.consumers.read().cancel_key(id).map(Outcome::Consumer),
            lanes::Kind::Confirm => {
                if !self.control.has_confirms() {
                    return None;
                }
                match self.confirm.reap(id) {
                    confirms::Reap::Removed => Some(Outcome::Confirm),
                    confirms::Reap::Missing => None,
                }
            }
            lanes::Kind::Publish => {
                let key = streams::Key::new(id);
                self.publishing.get(key)?.finish();
                Some(Outcome::Publish)
            }
        }
    }
}
