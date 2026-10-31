use std::{mem, num};

use o3::depot::arena::hash::runtime::entry;
use o3::depot::map::Vacancy;
use o3::mem::rc;

use crate::failure::wire::turn;
use crate::failure::{self, wire};
use crate::protocol::topology::records::consumer::Record;
use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::text::{resident, short};
use crate::state::channel::consumer;
use crate::state::channel::consumer::replay;
use crate::state::channel::consumer::storage::registry::Registry;
use crate::state::channel::consumer::storage::slot::Slot;
use crate::state::channel::consumer::storage::status::Status;

pub(crate) struct Write<'a, 'b, 'd, const ID: u8> {
    registry: &'a mut Registry<'b, 'd, ID>,
}

pub(crate) struct Named<'a, 'b, 'd, const ID: u8> {
    entry: Vacancy<entry::Live<'a, Slot<'b, 'd, ID>, consumer::Tag, entry::Staged>, u64>,
}

pub(crate) struct Fresh<'a, 'b, 'd, 'n, const ID: u8> {
    entry: Vacancy<entry::Live<'a, Slot<'b, 'd, ID>, consumer::Tag, entry::Staged>, u64>,
    name: short::Short<'n>,
}

pub(crate) enum Fault<'b, 'd> {
    Mismatch(consumer::Mismatch<'b, 'd>),
    Protocol(wire::Fault),
    Retain(rc::Error),
}

impl<'b, 'd> From<consumer::Mismatch<'b, 'd>> for Fault<'b, 'd> {
    fn from(error: consumer::Mismatch<'b, 'd>) -> Self {
        Self::Mismatch(error)
    }
}

impl<'b, 'd, const ID: u8> Named<'_, 'b, 'd, ID> {
    pub(crate) fn try_commit(self) -> Result<resident::Short<'b, 'd>, rc::Error> {
        let tag = self.entry.get().spec.try_tag()?;
        self.entry.commit().into_mut().status = Status::Active;
        Ok(tag)
    }

    pub(crate) fn retire(self) {
        self.entry.commit().into_mut().status = Status::Retiring;
    }
}

impl<'b, 'd, const ID: u8> Fresh<'_, 'b, 'd, '_, ID> {
    pub(crate) fn commit(mut self, writer: resident::Writer<'b>) -> resident::Short<'b, 'd> {
        let (stored, tag) = writer.commit_pair(self.name);
        let slot = self.entry.get_mut();
        slot.spec.set_tag(stored);
        self.entry.commit().into_mut().status = Status::Active;
        tag.into()
    }

    pub(crate) fn retire(mut self, writer: resident::Writer<'b>) {
        let tag = writer.commit(self.name);
        let slot = self.entry.get_mut();
        slot.spec.set_tag(tag);
        self.entry.commit().into_mut().status = Status::Retiring;
    }
}

impl<'b, 'a, 'd, const ID: u8> Write<'a, 'b, 'd, ID> {
    pub(in crate::state::channel::consumer) const fn new(
        registry: &'a mut Registry<'b, 'd, ID>,
    ) -> Self {
        Self { registry }
    }

    pub(crate) fn insert(
        &mut self,
        spec: Record<'b, 'd, ID>,
    ) -> Result<consumer::Key, failure::Error> {
        self.registry.check(spec.tag())?;
        match self.registry.slots.insert(Slot::pending(spec)) {
            Ok(key) => Ok(consumer::Key::from_raw(key)),
            Err(_) => Err(failure::Error::Capacity(wire::Capacity::Consumer)),
        }
    }

    pub(crate) fn named(
        self,
        key: consumer::Key,
        reply: short::Short<'_>,
    ) -> Result<Named<'a, 'b, 'd, ID>, Fault<'b, 'd>> {
        let entry = self
            .registry
            .try_entry(key, turn::Step::Pending)
            .map_err(Fault::Protocol)?;
        let slot = entry.get();
        if slot.status != Status::Pending {
            return Err(Fault::Protocol(wire::Fault::ConsumerPhase {
                consumer: num::NonZero::from(key).get(),
                expected: turn::Consumer::Pending,
                actual: slot.status.phase(),
            }));
        }
        let expected = slot.spec.tag();
        if expected.is_empty() || expected.value() != reply.value() {
            return Err(
                consumer::Mismatch::Tag(slot.spec.try_tag().map_err(Fault::Retain)?).into(),
            );
        }
        let staged = match entry {
            entry::Entry::Staged(staged) => staged,
            entry::Entry::Indexed(indexed) => {
                return Err(Fault::Protocol(wire::Fault::ConsumerIndex {
                    consumer: indexed.key().parts().into_raw().get(),
                    expected: turn::Index::Staged,
                    actual: turn::Index::Indexed,
                }));
            }
        };
        let entry = staged
            .try_reserve(
                Fingerprint::of(reply.value().as_bytes()).get(),
                reply.value(),
            )
            .map_err(|(staged, actual)| {
                Fault::Mismatch(consumer::Mismatch::Collision(turn::Clash::new(
                    consumer::Key::from_raw(staged.key()),
                    consumer::Key::from_raw(actual),
                )))
            })?;
        Ok(Named { entry })
    }

    pub(crate) fn fresh<'n>(
        self,
        key: consumer::Key,
        tag: short::Short<'n>,
    ) -> Result<Fresh<'a, 'b, 'd, 'n, ID>, Fault<'b, 'd>> {
        let entry = self
            .registry
            .try_entry(key, turn::Step::Pending)
            .map_err(Fault::Protocol)?;
        let slot = entry.get();
        if slot.status != Status::Pending {
            return Err(Fault::Protocol(wire::Fault::ConsumerPhase {
                consumer: num::NonZero::from(key).get(),
                expected: turn::Consumer::Pending,
                actual: slot.status.phase(),
            }));
        }
        if !slot.spec.tag().is_empty() {
            return Err(
                consumer::Mismatch::Tag(slot.spec.try_tag().map_err(Fault::Retain)?).into(),
            );
        }
        let staged = match entry {
            entry::Entry::Staged(staged) => staged,
            entry::Entry::Indexed(indexed) => {
                return Err(Fault::Protocol(wire::Fault::ConsumerIndex {
                    consumer: indexed.key().parts().into_raw().get(),
                    expected: turn::Index::Staged,
                    actual: turn::Index::Indexed,
                }));
            }
        };
        let entry = staged
            .try_reserve(Fingerprint::of(tag.value().as_bytes()).get(), tag.value())
            .map_err(|(staged, actual)| {
                Fault::Mismatch(consumer::Mismatch::Collision(turn::Clash::new(
                    consumer::Key::from_raw(staged.key()),
                    consumer::Key::from_raw(actual),
                )))
            })?;
        Ok(Fresh { entry, name: tag })
    }

    pub(crate) fn begin_replay(
        &mut self,
        token: consumer::Key,
    ) -> Result<consumer::Key, failure::Error> {
        let id = num::NonZero::from(token);
        let entry = self
            .registry
            .try_entry(token, turn::Step::Replay)
            .map_err(failure::Error::Protocol)?;
        let mut entry = match entry {
            entry::Entry::Indexed(entry) => entry,
            entry::Entry::Staged(staged) => {
                return Err(failure::Error::Protocol(wire::Fault::ConsumerIndex {
                    consumer: staged.key().parts().into_raw().get(),
                    expected: turn::Index::Indexed,
                    actual: turn::Index::Staged,
                }));
            }
        };
        if entry.get().status != Status::Recovering {
            return Err(failure::Error::Protocol(wire::Fault::ConsumerPhase {
                consumer: id.get(),
                expected: turn::Consumer::Recovering,
                actual: entry.get().status.phase(),
            }));
        }
        entry.get_mut().status = Status::Replaying;
        Ok(token)
    }

    pub(crate) fn finish(
        &mut self,
        key: consumer::Key,
        tag: short::Short<'_>,
    ) -> Result<replay::Completion<'b, 'd>, Fault<'b, 'd>> {
        let mut entry = self
            .registry
            .try_entry(key, turn::Step::Replay)
            .map_err(Fault::Protocol)?;
        if entry.get().tag().value() != tag.value() {
            return Err(consumer::Mismatch::Tag(
                entry.get().spec.try_tag().map_err(Fault::Retain)?,
            )
            .into());
        }
        match entry.get().status {
            Status::Replaying => {
                entry.get_mut().status = Status::Active;
                Ok(replay::Completion::Active)
            }
            Status::Retiring => Ok(replay::Completion::Retiring),
            Status::Canceled => Ok(replay::Completion::Canceled(
                entry.get().spec.try_tag().map_err(Fault::Retain)?,
            )),
            Status::Pending | Status::Active | Status::Untracked | Status::Recovering => {
                Err(Fault::Protocol(wire::Fault::ConsumerPhase {
                    consumer: num::NonZero::from(key).get(),
                    expected: turn::Consumer::Replaying,
                    actual: entry.get().status.phase(),
                }))
            }
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Write<'static, 'static, 'static, 0>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Named<'static, 'static, 'static, 0>>() == 4 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Fresh<'static, 'static, 'static, 'static, 0>>()
            == 6 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Result<Fresh<'static, 'static, 'static, 'static, 0>, Fault<'static, 'static>>>(
        ) == 6 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Result<Named<'static, 'static, 'static, 0>, Fault<'static, 'static>>>()
            == 5 * mem::size_of::<usize>()
    );
};
