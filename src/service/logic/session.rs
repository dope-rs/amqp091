use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::clock;
use dope::manifold::client::{self, close};
use dope::manifold::service::bindings::endpoint::State;
use dope::net::link::egress;
use o3::base::cell::{local, region};
use o3::buffer::{self, hold, pool};

use crate::failure::{self, recovery, wire};
use crate::protocol::method;
use crate::protocol::method::conn::CloseRef;
use crate::protocol::wire::frame;
use crate::protocol::wire::text::resident;
use crate::service::content::ops::{FailureOwner, Input, Ops as _};
use crate::service::content::wait::Attempt;
use crate::service::method::Ops as _;
use crate::service::{self, port};
use crate::state::channel::{self, change};
use crate::state::conn::{self, phase, status};
use crate::state::{channels, epoch, shared};

pub(crate) trait SessionOps<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    fn port(&self) -> &'o port::Domain<'r, 'c, 'b, 'd, ID>;
    fn with_shared<R>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        f: impl for<'a> FnOnce(&'a mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> R;
    fn with_channel<R>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        id: u16,
        f: impl for<'a> FnOnce(&'a mut channel::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Option<R>;
    fn with_lease<R>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        lease: channels::Lease<'d, ID>,
        f: impl for<'a> FnOnce(&'a mut channel::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Option<R>;
    fn send_method<S: method::Encode>(
        &self,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
        id: u16,
        method: &S,
    ) -> Result<(), failure::Error>;
    fn send_pair<A: method::Encode, B: method::Encode>(
        &self,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
        id: u16,
        first: &A,
        second: &B,
    ) -> Result<(), failure::Error>;
    fn enqueue(
        &self,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
        frame: pool::Cursor,
    ) -> Result<(), failure::Error> {
        match out.try_load(region, egress::Load::new(frame)) {
            egress::Enqueue::Committed => Ok(()),
            egress::Enqueue::Rejected(reject) => {
                let (load, error) = reject.into_parts();
                load.into_inner().recycle();
                service::Admission::new(self.port()).sync();
                Err(failure::Error::Egress(error))
            }
        }
    }
    fn start_close(
        &mut self,
        close: &mut impl FnMut(close::Directive),
        epoch: epoch::Id<'d, ID>,
        error: failure::Error<resident::Short<'b, 'd>>,
        reply: wire::Reply,
        sink: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    );
    fn connect(
        &mut self,
        epoch: epoch::Id<'d, ID>,
        state: &mut conn::Transport,
        close: &mut impl FnMut(close::Directive),
        sink: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        until: Option<clock::Deadline<'d>>,
        region: &mut region::Token<region::Identity<'d>>,
    );
    fn respond<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        head: frame::Inbound<'input, 'd, R>,
        turn: Attempt<'d, ID>,
        parts: &mut service::Turn<'_, '_, 'd, ID>,
    ) -> Input<'d, ID>;
    fn disconnect(
        &mut self,
        epoch: epoch::Id<'d, ID>,
        reason: close::Reason,
        region: &mut region::Token<region::Identity<'d>>,
    );
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> SessionOps<'o, 'r, 'c, 'b, 'd, ID>
    for State<'o, 'd, service::Port<'r, 'c, 'b, 'd, ID>, service::Logic<'o, 'd, ID>, KeyTag<ID>, 1>
{
    fn port(&self) -> &'o port::Domain<'r, 'c, 'b, 'd, ID> {
        self.root().domain()
    }

    fn with_shared<R>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        f: impl for<'a> FnOnce(&'a mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> R {
        self.port().session.write(local::Access::new(region), f)
    }

    fn with_channel<R>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        id: u16,
        f: impl for<'a> FnOnce(&'a mut channel::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Option<R> {
        self.with_shared(region, |shared| {
            shared.routes.with_id(id, |view| f(view.channel))
        })
    }

    fn with_lease<R>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        lease: channels::Lease<'d, ID>,
        f: impl for<'a> FnOnce(&'a mut channel::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Option<R> {
        self.with_shared(region, |shared| {
            shared.routes.project(lease, |view| f(view.channel))
        })
    }

    fn send_method<S: method::Encode>(
        &self,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
        id: u16,
        m: &S,
    ) -> Result<(), failure::Error> {
        let frame = self.port().encode(|frame| m.write(frame, id))?;
        self.enqueue(out, region, frame)
    }

    fn send_pair<A: method::Encode, B: method::Encode>(
        &self,
        out: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
        id: u16,
        first: &A,
        second: &B,
    ) -> Result<(), failure::Error> {
        let frame = self.port().encode(|frame| {
            first.write(frame, id);
            second.write(frame, id);
        })?;
        self.enqueue(out, region, frame)
    }

    fn start_close(
        &mut self,
        close: &mut impl FnMut(close::Directive),
        epoch: epoch::Id<'d, ID>,
        error: failure::Error<resident::Short<'b, 'd>>,
        reply: wire::Reply,
        sink: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let cause =
            match self.with_shared(region, |shared| shared.connection.try_fail(epoch, error)) {
                Ok(cause) => cause,
                Err(error) => {
                    self.fail_session(
                        close,
                        FailureOwner::Immediate(epoch),
                        recovery::Fault::terminal(error),
                        region,
                    );
                    return;
                }
            };
        let stores = self.port().content.stores();
        let freed = self.with_shared(region, |shared| {
            let mut freed = channel::Freed::Available;
            shared.routes.for_each(|chan, graph| {
                freed = freed.merge(chan.change(
                    graph,
                    stores,
                    change::Event::TransportEnded { cause: &cause },
                ));
            });
            freed
        });
        self.port().coordination().waiters().wake_all();
        self.port().admission().wake_all();
        if let Err(error) = self.wake_route(epoch, freed) {
            self.fail_session(
                close,
                FailureOwner::Immediate(epoch),
                recovery::Fault::terminal(error.into()),
                region,
            );
            return;
        }
        let code = reply.code();
        let text = reply.text();
        if let Err(error) = self.send_method(
            sink,
            region,
            0,
            &CloseRef {
                reply_code: code,
                reply_text: text,
                class_id: 0,
                method_id: 0,
            },
        ) {
            self.fail_session(
                close,
                FailureOwner::Immediate(epoch),
                recovery::Fault::session(error),
                region,
            );
        }
    }

    fn connect(
        &mut self,
        epoch: epoch::Id<'d, ID>,
        state: &mut conn::Transport,
        close: &mut impl FnMut(close::Directive),
        sink: &mut egress::Queue<'_, '_, 'd, { client::IOV_CAP }, pool::Cursor>,
        until: Option<clock::Deadline<'d>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let Some(slot) = self.port().causes().reserve() else {
            let failed = self.with_shared(region, |shared| shared.reject(None));
            close(
                status::Retirement::Permanent
                    .abort(close::Reason::Capacity(close::Capacity::Transport)),
            );
            match failed {
                conn::Change::Applied => {
                    self.port().coordination().waiters().wake_state();
                    self.port().coordination().waiters().wake_closed();
                }
                conn::Change::Ignored => {}
            }
            return;
        };
        let stores = self.port().content.stores();
        match self.with_shared(region, |shared| shared.begin(epoch, slot, until, stores)) {
            shared::Start::Ignored => {
                close(
                    status::Retirement::Permanent
                        .abort(close::Reason::Local(close::LocalOrigin::Superseded)),
                );
                return;
            }
            shared::Start::Applied => {}
            shared::Start::Route { epoch } => {
                if let Err(error) = self.port().content.wake_route(epoch.token()) {
                    self.fail_session(
                        close,
                        FailureOwner::Immediate(epoch),
                        recovery::Fault::terminal(error.into()),
                        region,
                    );
                    return;
                }
            }
        }
        let admit = self
            .port()
            .encode(|frame| frame::Encoder::new(frame).preface())
            .and_then(|frame| self.enqueue(sink, region, frame));
        if let Err(error) = admit {
            self.fail_session(
                close,
                FailureOwner::Immediate(epoch),
                recovery::Fault::session(error),
                region,
            );
            return;
        }
        state.phase = phase::Phase::Start;
    }

    fn respond<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        head: frame::Inbound<'input, 'd, R>,
        turn: Attempt<'d, ID>,
        parts: &mut service::Turn<'_, '_, 'd, ID>,
    ) -> Input<'d, ID> {
        let epoch = epoch::Id::new(parts.conn_id);
        let (valid, closing) = self.with_shared(parts.driver.region_token(), |shared| {
            let state = shared.connection.lifecycle.status();
            (
                state.can_accept(epoch, status::Input::Frame),
                state.is_closing(),
            )
        });
        if !valid {
            parts.close.request(
                status::Retirement::Permanent
                    .abort(close::Reason::Local(close::LocalOrigin::Superseded)),
            );
            return Input::Committed;
        }
        let result = match head {
            frame::Inbound::Header { .. } | frame::Inbound::Body { .. } if closing => {
                Ok(Input::Committed)
            }
            frame::Inbound::Method {
                channel: id,
                payload: body,
            } => self.recv(id, body, closing, turn, parts),
            frame::Inbound::Header {
                channel: id,
                payload: body,
            } => self.header(id, body, turn, parts.driver.region_token()),
            frame::Inbound::Body {
                channel: id,
                payload: body,
            } => self.body(id, body, turn, parts.driver.region_token()),
            frame::Inbound::Heartbeat => Ok(Input::Committed),
            frame::Inbound::Fault(fault) => Err(failure::Error::Frame(fault)),
        };
        match result {
            Ok(input) => input,
            Err(error) => {
                let mut close = |next| parts.close.request(next);
                let region = parts.driver.region_token();
                if parts.state.phase == phase::Phase::Ready
                    && let Some(reply) = error.close_reply()
                {
                    self.start_close(
                        &mut close,
                        epoch,
                        error.into(),
                        reply,
                        &mut parts.sink,
                        region,
                    );
                    return Input::Committed;
                }
                self.fail_session(
                    &mut close,
                    FailureOwner::Immediate(epoch),
                    recovery::Fault::session(error),
                    region,
                );
                Input::Committed
            }
        }
    }

    fn disconnect(
        &mut self,
        epoch: epoch::Id<'d, ID>,
        reason: close::Reason,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        let change = self.with_shared(region, |shared| {
            shared.disconnect(epoch, reason.into(), self.port().content.stores())
        });
        match change {
            shared::Disconnect::Ignored => return,
            shared::Disconnect::Applied {
                freed: channel::Freed::Available | channel::Freed::Route,
            } => {}
        }
        if let Err(error) = self.port().content.cancel(epoch.token()) {
            self.port().fail(local::Access::new(region), epoch, error);
        }
        self.port().coordination().waiters().wake_all();
        self.port().admission().wake_all();
    }
}
