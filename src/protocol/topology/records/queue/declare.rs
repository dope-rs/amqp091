use o3::mem::rc;

use crate::protocol::topology::{State, store};
use crate::protocol::wire::schema::queues;
use crate::protocol::wire::text::{resident, short};
use crate::protocol::wire::{self, bytes};

pub(crate) struct Declare<'b, 'd> {
    record: bytes::Bytes<'d, queues::Declaration>,
    pub(in crate::protocol::topology) server_name: Option<resident::Identity<'b>>,
}

impl<'c, 'b, 'd, const ID: u8> State<'_, 'c, 'b, 'd, ID> {
    pub(crate) fn declare(
        &mut self,
        record: bytes::Bytes<'d, queues::Declaration>,
        plan: store::QueueHold<'c, 'b, 'd, ID>,
        server_name: Option<resident::Identity<'b>>,
    ) -> super::Key<'d, ID> {
        let record = Declare {
            record,
            server_name,
        };
        let key = self.topology.queues.insert(self.values, plan, record);
        self.local.current = super::Current::Tracked(key);
        key
    }
}

impl<'b, 'd> Declare<'b, 'd> {
    pub(crate) fn user_name(&self) -> short::Name<'_> {
        self.record.name()
    }

    pub(crate) fn server_name(&self) -> short::Name<'_> {
        match self.server_name.as_ref() {
            Some(name) => {
                // SAFETY: server_name is installed only from the admitted Name
                // in Flight::retain_queue or Refresh::commit; retention preserves it.
                unsafe { short::raw::name(short::Short::from(name)) }
            }
            None => self.user_name(),
        }
    }

    pub(in crate::protocol::topology) fn hash(&self) -> u64 {
        match self.server_name.as_ref() {
            Some(name) => name.hash(),
            None => wire::Fingerprint::of(self.user_name().value().as_bytes()).get(),
        }
    }

    pub(crate) fn try_name(&self) -> Result<resident::Short<'b, 'd>, rc::Error> {
        match self.server_name.as_ref() {
            Some(name) => Ok(name.try_clone()?.into()),
            None => self.record.try_field(queues::Declaration::QUEUE),
        }
    }

    pub(in crate::protocol) const fn wire(&self) -> &bytes::Bytes<'d, queues::Declaration> {
        &self.record
    }
}
