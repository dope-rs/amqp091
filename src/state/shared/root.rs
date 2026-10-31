use dope::core::driver;
use o3::base::cell::{local, region};
use o3::depot::arena::slab;

use crate::protocol::topology::records::queue;
use crate::state::{channels, shared};

pub(crate) struct Root<'r, 'c, 'b, 'd, const ID: u8> {
    session: region::Value<region::Identity<'d>, shared::Session<'r, 'c, 'b, 'd, ID>>,
    queues: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Root<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn try_new(
        plan: channels::Plan,
        queues: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        driver: driver::Reference<'d>,
        region: &region::Token<region::Identity<'d>>,
        permits: &crate::permits::Registry<'c, 'd>,
    ) -> Result<Self, channels::Error> {
        let session = shared::Session::try_new(plan, driver, permits)?;
        Ok(Self {
            session: region.bind(session),
            queues,
        })
    }

    pub(crate) fn read<R>(
        &self,
        access: local::Read<'_, region::Identity<'d>>,
        f: impl FnOnce(&shared::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> R {
        self.session.read(access, f)
    }

    pub(crate) fn write<R>(
        &self,
        access: local::Access<'_, region::Identity<'d>>,
        f: impl for<'a> FnOnce(&mut shared::Edit<'a, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> R {
        self.session.write(access, |session| {
            let mut state = shared::Edit {
                connection: &mut session.connection,
                recovery: &mut session.recovery,
                routes: session.routes.edit(self.queues),
            };
            f(&mut state)
        })
    }
}
