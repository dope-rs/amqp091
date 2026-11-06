use crate::message::ack;
use crate::protocol::method::{basic, channel, confirm, queue, tx};

pub trait Seal {}

impl Seal for channel::Open {}
impl Seal for channel::Close<'_> {}
impl Seal for channel::CloseRef<'_> {}
impl Seal for tx::Select {}
impl Seal for tx::Commit {}
impl Seal for tx::Rollback {}
impl Seal for basic::Recover {}
impl Seal for queue::Inspect<'_> {}
impl Seal for queue::Purge<'_> {}
impl Seal for basic::Qos {}
impl Seal for confirm::Select {}
impl<M: ack::Mode> Seal for basic::Consume<'_, M> {}
impl Seal for queue::Declare<'_> {}
impl Seal for queue::Bind<'_> {}
impl Seal for queue::Unbind<'_> {}
impl Seal for queue::Delete<'_> {}
