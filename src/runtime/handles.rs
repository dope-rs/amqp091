use dope::WakeRef;
use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::transport::Transport;
use o3::buffer::Owned;

use crate::Error;
use crate::content::BasicProperties;
use crate::method::{ClientMethod, basic, channel, confirm, exchange, queue, tx};
use crate::protocol::{ExpectMethod, Session};
use crate::runtime::backpressure::{WaitFlowResumed, WaitUnblocked};
use crate::runtime::consumer::Consumer;
use crate::runtime::handshake::Handshake;
use crate::runtime::method_future::MethodFuture;
use crate::runtime::publish_future::PublishConfirmFuture;
use crate::runtime::returns::NextReturn;
use crate::topology::{
    ConsumerSpec, ExchangeBindSpec, ExchangeDecl, QosSpec, QueueBindSpec, QueueDecl,
};
use crate::wire::{FieldTable, FrameEncoder, FrameType, OVERHEAD};

pub type AmqpHolding<'d, S, E> = Holding<'d, Connector<0, Session, S, E>>;

pub struct Connection<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: AmqpHolding<'d, S, E>,
}

impl<'d, S, E> Clone for Connection<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn clone(&self) -> Self {
        Self {
            holding: self.holding,
        }
    }
}

impl<'d, S, E> Connection<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn from_holding(holding: AmqpHolding<'d, S, E>) -> Self {
        Self { holding }
    }

    pub fn holding(&self) -> AmqpHolding<'d, S, E> {
        self.holding
    }

    pub fn handshake(&self) -> Handshake<'d, S, E> {
        Handshake::new(self.holding)
    }

    pub fn revive_connector(&self) {
        let mut h = self.holding.hold();
        h.as_mut().session_mut().shared.clear_fatal();
        h.as_mut().revive_upstreams();
    }

    pub fn wait_unblocked(&self) -> WaitUnblocked<'d, S, E> {
        WaitUnblocked::new(self.holding)
    }

    pub fn is_blocked(&self) -> bool {
        self.holding.session().shared.is_blocked()
    }

    pub fn is_ready(&self) -> bool {
        self.holding.session().shared.ready_conn.is_some()
    }

    pub fn is_failed(&self) -> bool {
        self.holding.session().shared.fatal.is_some()
    }

    pub fn channel(&self, id: u16) -> Channel<'d, S, E> {
        Channel {
            holding: self.holding,
            channel: id,
        }
    }

    pub fn open_channel(&self, id: u16) -> MethodFuture<'d, S, E, ()> {
        self.holding
            .hold()
            .as_mut()
            .session_mut()
            .shared
            .reserve_channel(id);
        let mut buf = Owned::with_capacity(16);
        channel::Open.write_frame(&mut buf, id);
        MethodFuture::new(self.holding, id, ExpectMethod::ChannelOpenOk, buf.freeze())
    }

    pub async fn open_and_channel(&self, id: u16) -> Result<Channel<'d, S, E>, Error> {
        self.open_channel(id).await?;
        Ok(self.channel(id))
    }

    pub async fn open_auto_channel(&self) -> Result<Channel<'d, S, E>, Error> {
        let id = {
            let mut h = self.holding.hold();
            let channel_max = if let Some(target) = h.session().shared.ready_conn
                && let Some(slot) = h.as_mut().state_for(target)
            {
                slot.conn_state().negotiated.channel_max
            } else {
                0
            };
            h.as_mut()
                .session_mut()
                .shared
                .next_unused_channel(channel_max)
                .ok_or_else(|| Error::Other("no free channel id available".into()))?
        };
        let mut buf = Owned::with_capacity(16);
        channel::Open.write_frame(&mut buf, id);
        let fut = MethodFuture::<'d, S, E, ()>::new(
            self.holding,
            id,
            ExpectMethod::ChannelOpenOk,
            buf.freeze(),
        );
        match fut.await {
            Ok(()) => Ok(self.channel(id)),
            Err(e) => {
                self.holding
                    .hold()
                    .as_mut()
                    .session_mut()
                    .shared
                    .release_channel(id);
                Err(e)
            }
        }
    }

    pub fn close_now(&self, reply_code: u16, reply_text: &str) {
        let mut h = self.holding.hold();
        h.as_mut().session_mut().shared.mark_intentional_close();
        if let Some(target) = h.session().shared.ready_conn
            && let Some(slot) = h.as_mut().state_for(target)
        {
            let mut buf = Owned::with_capacity(64);
            crate::method::connection::Close {
                reply_code,
                reply_text: reply_text.into(),
                class_id: 0,
                method_id: 0,
            }
            .write_frame(&mut buf, 0);
            let _ = slot.enqueue(buf.freeze());
            h.as_mut().request_flush(target);
        }
    }

    pub async fn close(&self, reply_code: u16, reply_text: &str) -> Result<(), Error> {
        let holding = self.holding;
        self.close_now(reply_code, reply_text);
        std::future::poll_fn(move |cx| {
            let mut h = holding.hold();
            if h.session().shared.ready_conn.is_none() {
                return std::task::Poll::Ready(Ok(()));
            }
            if let Some(err) = h.session().shared.fatal.as_ref() {
                return std::task::Poll::Ready(Err(err.clone()));
            }
            h.as_mut()
                .session_mut()
                .shared
                .register_closed_waker(WakeRef::verified(cx.waker()));
            std::task::Poll::Pending
        })
        .await
    }
}

pub struct Channel<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: AmqpHolding<'d, S, E>,
    channel: u16,
}

impl<'d, S, E> Clone for Channel<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn clone(&self) -> Self {
        Self {
            holding: self.holding,
            channel: self.channel,
        }
    }
}

impl<'d, S, E> Channel<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn id(&self) -> u16 {
        self.channel
    }

    pub fn holding(&self) -> AmqpHolding<'d, S, E> {
        self.holding
    }

    pub fn wait_flow_resumed(&self) -> WaitFlowResumed<'d, S, E> {
        WaitFlowResumed::new(self.holding, self.channel)
    }

    pub fn next_return(&self) -> NextReturn<'d, S, E> {
        NextReturn::new(self.holding, self.channel)
    }

    fn send_for<M: ClientMethod, R>(
        &self,
        expect: ExpectMethod,
        method: M,
    ) -> MethodFuture<'d, S, E, R>
    where
        R: crate::runtime::extract::Extract,
    {
        let mut buf = Owned::with_capacity(64);
        method.write_frame(&mut buf, self.channel);
        MethodFuture::new(self.holding, self.channel, expect, buf.freeze())
    }

    pub fn close(&self, reply_code: u16, reply_text: &str) -> MethodFuture<'d, S, E, ()> {
        self.send_for::<_, ()>(
            ExpectMethod::ChannelCloseOk,
            channel::Close {
                reply_code,
                reply_text: reply_text.into(),
                class_id: 0,
                method_id: 0,
            },
        )
    }

    pub async fn exchange_declare(&self, opts: ExchangeDeclareOpts<'_>) -> Result<(), Error> {
        let spec = ExchangeDecl {
            name: opts.exchange.to_string(),
            kind: opts.kind.to_string(),
            durable: opts.durable,
            auto_delete: opts.auto_delete,
            internal: opts.internal,
            arguments: opts.arguments.clone(),
        };
        let fut = self.send_for::<_, ()>(
            ExpectMethod::ExchangeDeclareOk,
            exchange::Declare {
                exchange: opts.exchange,
                kind: opts.kind,
                passive: opts.passive,
                durable: opts.durable,
                auto_delete: opts.auto_delete,
                internal: opts.internal,
                no_wait: false,
                arguments: opts.arguments,
            },
        );
        fut.await?;
        if !opts.passive {
            self.with_topology(|t, ch| t.record_exchange(ch, spec));
        }
        Ok(())
    }

    pub async fn exchange_delete(&self, name: &str, if_unused: bool) -> Result<(), Error> {
        let owned = name.to_string();
        let fut = self.send_for::<_, ()>(
            ExpectMethod::ExchangeDeleteOk,
            exchange::Delete {
                exchange: name,
                if_unused,
                no_wait: false,
            },
        );
        fut.await?;
        self.with_topology(|t, ch| t.forget_exchange(ch, &owned));
        Ok(())
    }

    pub async fn exchange_bind(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> Result<(), Error> {
        let spec = ExchangeBindSpec {
            destination: destination.to_string(),
            source: source.to_string(),
            routing_key: routing_key.to_string(),
            arguments: arguments.clone(),
        };
        let fut = self.send_for::<_, ()>(
            ExpectMethod::ExchangeBindOk,
            exchange::Bind {
                destination,
                source,
                routing_key,
                no_wait: false,
                arguments,
            },
        );
        fut.await?;
        self.with_topology(|t, ch| t.record_exchange_bind(ch, spec));
        Ok(())
    }

    pub fn exchange_unbind(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> MethodFuture<'d, S, E, ()> {
        self.send_for(
            ExpectMethod::ExchangeUnbindOk,
            exchange::Unbind {
                destination,
                source,
                routing_key,
                no_wait: false,
                arguments,
            },
        )
    }

    pub async fn queue_declare(
        &self,
        opts: QueueDeclareOpts<'_>,
    ) -> Result<queue::DeclareOk, Error> {
        let user_name = opts.queue.to_string();
        let durable = opts.durable;
        let exclusive = opts.exclusive;
        let auto_delete = opts.auto_delete;
        let args = opts.arguments.clone();
        let passive = opts.passive;
        let fut = self.send_for::<_, queue::DeclareOk>(
            ExpectMethod::QueueDeclareOk,
            queue::Declare {
                queue: opts.queue,
                passive: opts.passive,
                durable: opts.durable,
                exclusive: opts.exclusive,
                auto_delete: opts.auto_delete,
                no_wait: false,
                arguments: opts.arguments,
            },
        );
        let ok = fut.await?;
        if !passive {
            self.with_topology(|t, ch| {
                t.record_queue(
                    ch,
                    QueueDecl {
                        user_name,
                        server_name: ok.queue.clone(),
                        durable,
                        exclusive,
                        auto_delete,
                        arguments: args,
                    },
                )
            });
        }
        Ok(ok)
    }

    pub async fn queue_bind(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> Result<(), Error> {
        let queue_user_name = self.with_topology(|t, ch| t.canonical_queue_user_name(ch, queue));
        let spec = QueueBindSpec {
            queue_user_name,
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            arguments: arguments.clone(),
        };
        let resolved_queue = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, queue))
            .unwrap_or_else(|| queue.to_string());
        let fut = self.send_for::<_, ()>(
            ExpectMethod::QueueBindOk,
            queue::Bind {
                queue: &resolved_queue,
                exchange,
                routing_key,
                no_wait: false,
                arguments,
            },
        );
        fut.await?;
        self.with_topology(|t, ch| t.record_queue_bind(ch, spec));
        Ok(())
    }

    pub fn queue_unbind(
        &self,
        queue_name: &str,
        exchange: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> MethodFuture<'d, S, E, ()> {
        let resolved = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, queue_name))
            .unwrap_or_else(|| queue_name.to_string());
        let mut buf = Owned::with_capacity(64);
        queue::Unbind {
            queue: &resolved,
            exchange,
            routing_key,
            arguments,
        }
        .write_frame(&mut buf, self.channel);
        MethodFuture::new(
            self.holding,
            self.channel,
            ExpectMethod::QueueUnbindOk,
            buf.freeze(),
        )
    }

    pub fn queue_purge(&self, queue_name: &str) -> MethodFuture<'d, S, E, queue::PurgeOk> {
        let resolved = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, queue_name))
            .unwrap_or_else(|| queue_name.to_string());
        let mut buf = Owned::with_capacity(64);
        queue::Purge {
            queue: &resolved,
            no_wait: false,
        }
        .write_frame(&mut buf, self.channel);
        MethodFuture::new(
            self.holding,
            self.channel,
            ExpectMethod::QueuePurgeOk,
            buf.freeze(),
        )
    }

    pub async fn queue_delete(
        &self,
        queue_name: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<queue::DeleteOk, Error> {
        let user_name = queue_name.to_string();
        let resolved = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, queue_name))
            .unwrap_or_else(|| queue_name.to_string());
        let fut = self.send_for::<_, queue::DeleteOk>(
            ExpectMethod::QueueDeleteOk,
            queue::Delete {
                queue: &resolved,
                if_unused,
                if_empty,
                no_wait: false,
            },
        );
        let ok = fut.await?;
        self.with_topology(|t, ch| t.forget_queue(ch, &user_name));
        Ok(ok)
    }

    pub async fn basic_qos(
        &self,
        prefetch_size: u32,
        prefetch_count: u16,
        global: bool,
    ) -> Result<(), Error> {
        let spec = QosSpec {
            prefetch_size,
            prefetch_count,
            global,
        };
        let fut = self.send_for::<_, ()>(
            ExpectMethod::BasicQosOk,
            basic::Qos {
                prefetch_size,
                prefetch_count,
                global,
            },
        );
        fut.await?;
        self.with_topology(|t, ch| t.record_qos(ch, spec));
        Ok(())
    }

    pub async fn basic_consume(&self, opts: ConsumeOpts<'_>) -> Result<Consumer<'d, S, E>, Error> {
        let user_name = self.with_topology(|t, ch| t.canonical_queue_user_name(ch, opts.queue));
        let resolved_queue = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, opts.queue))
            .unwrap_or_else(|| opts.queue.to_string());
        let no_local = opts.no_local;
        let no_ack = opts.no_ack;
        let exclusive = opts.exclusive;
        let args = opts.arguments.clone();
        let fut = self.send_for::<_, basic::ConsumeOk>(
            ExpectMethod::BasicConsumeOk,
            basic::Consume {
                queue: &resolved_queue,
                consumer_tag: opts.consumer_tag,
                no_local: opts.no_local,
                no_ack: opts.no_ack,
                exclusive: opts.exclusive,
                no_wait: false,
                arguments: opts.arguments,
            },
        );
        let ok = fut.await?;
        self.with_topology(|t, ch| {
            t.record_consumer(
                ch,
                ConsumerSpec {
                    queue_user_name: user_name,
                    consumer_tag: ok.consumer_tag.clone(),
                    no_local,
                    no_ack,
                    exclusive,
                    arguments: args,
                },
            )
        });
        Ok(Consumer::new(self.holding, self.channel, ok.consumer_tag))
    }

    pub async fn basic_cancel(&self, consumer_tag: &str) -> Result<basic::CancelOk, Error> {
        let owned_tag = consumer_tag.to_string();
        let fut = self.send_for::<_, basic::CancelOk>(
            ExpectMethod::BasicCancelOk,
            basic::Cancel {
                consumer_tag: owned_tag.clone(),
                no_wait: false,
            },
        );
        let ok = fut.await?;
        self.with_topology(|t, ch| t.remove_consumer(ch, &owned_tag));
        Ok(ok)
    }

    pub fn basic_ack(&self, delivery_tag: u64, multiple: bool) -> bool {
        self.send_fire_forget(&basic::Ack {
            delivery_tag,
            multiple,
        })
    }

    pub fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> bool {
        self.send_fire_forget(&basic::Nack {
            delivery_tag,
            multiple,
            requeue,
        })
    }

    pub fn basic_reject(&self, delivery_tag: u64, requeue: bool) -> bool {
        self.send_fire_forget(&basic::Reject {
            delivery_tag,
            requeue,
        })
    }

    pub fn basic_recover(&self, requeue: bool) -> MethodFuture<'d, S, E, ()> {
        self.send_for(ExpectMethod::BasicRecoverOk, basic::Recover { requeue })
    }

    pub async fn confirm_select(&self) -> Result<(), Error> {
        let fut = self.send_for::<_, ()>(
            ExpectMethod::ConfirmSelectOk,
            confirm::Select { no_wait: false },
        );
        fut.await?;
        self.with_topology(|t, ch| t.record_confirm(ch));
        Ok(())
    }

    pub fn tx_select(&self) -> MethodFuture<'d, S, E, ()> {
        self.send_for(ExpectMethod::TxSelectOk, tx::Select)
    }

    pub fn tx_commit(&self) -> MethodFuture<'d, S, E, ()> {
        self.send_for(ExpectMethod::TxCommitOk, tx::Commit)
    }

    pub fn tx_rollback(&self) -> MethodFuture<'d, S, E, ()> {
        self.send_for(ExpectMethod::TxRollbackOk, tx::Rollback)
    }

    pub fn basic_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
    ) -> bool {
        let mut h = self.holding.hold();
        if h.session().shared.fatal.is_some() || h.session().shared.is_blocked() {
            return false;
        }
        let Some(target) = h.session().shared.ready_conn else {
            return false;
        };
        let Some(slot) = h.as_mut().state_for(target) else {
            return false;
        };
        let frame_max = slot.conn_state().negotiated.frame_max;
        if let Some(ch) = slot.conn_state_mut().channel_mut(self.channel)
            && !ch.flow_active()
        {
            return false;
        }
        let frames = self.build_publish_frames(
            exchange,
            routing_key,
            mandatory,
            properties,
            body,
            frame_max,
        );
        if !slot.enqueue_all(&frames) {
            return false;
        }
        h.as_mut().request_flush(target);
        true
    }

    fn build_publish_frames(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
        frame_max: u32,
    ) -> Vec<o3::buffer::Shared> {
        let max_body = Self::body_payload_max(frame_max);
        let body_frames = body.len().div_ceil(max_body);
        let mut frames = Vec::with_capacity(2 + body_frames);

        let mut method_buf = Owned::with_capacity(64);
        basic::Publish {
            exchange,
            routing_key,
            mandatory,
            immediate: false,
        }
        .write_frame(&mut method_buf, self.channel);
        frames.push(method_buf.freeze());

        let mut header_buf = Owned::with_capacity(64);
        FrameEncoder::write(&mut header_buf, FrameType::HEADER, self.channel, |s| {
            properties.encode_header_frame(s, body.len() as u64);
        });
        frames.push(header_buf.freeze());

        let mut off = 0;
        while off < body.len() {
            let end = (off + max_body).min(body.len());
            let mut frame_buf = Owned::with_capacity(end - off + 8);
            FrameEncoder::write_raw_body(&mut frame_buf, self.channel, &body[off..end]);
            frames.push(frame_buf.freeze());
            off = end;
        }
        frames
    }

    fn body_payload_max(frame_max: u32) -> usize {
        if frame_max == 0 {
            4096
        } else {
            (frame_max as usize).saturating_sub(OVERHEAD).max(1)
        }
    }

    pub fn basic_publish_confirm(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
    ) -> PublishConfirmFuture<'d, S, E> {
        let mut h = self.holding.hold();
        if let Some(err) = h.session().shared.fatal.as_ref() {
            return PublishConfirmFuture::failed(self.holding, self.channel, err.clone());
        }
        if h.session().shared.is_blocked() {
            let reason = h.session().shared.blocked_reason().to_string();
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::ConnectionBlocked(reason),
            );
        }
        let Some(target) = h.session().shared.ready_conn else {
            return PublishConfirmFuture::failed(self.holding, self.channel, Error::Closed);
        };
        let Some(slot) = h.as_mut().state_for(target) else {
            return PublishConfirmFuture::failed(self.holding, self.channel, Error::Closed);
        };
        let frame_max = slot.conn_state().negotiated.frame_max;
        let state = slot.conn_state_mut();
        let Some(ch) = state.channel_mut(self.channel) else {
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::Protocol("channel out of range"),
            );
        };
        if !ch.flow_active() {
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::ChannelFlowPaused,
            );
        }
        if !ch.confirm_enabled() {
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::Protocol("publisher confirms not enabled (call confirm_select first)"),
            );
        }
        let frames = self.build_publish_frames(
            exchange,
            routing_key,
            mandatory,
            properties,
            body,
            frame_max,
        );
        if !slot.enqueue_all(&frames) {
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::Other("egress queue full".into()),
            );
        }
        let Some(ch) = slot.conn_state_mut().channel_mut(self.channel) else {
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::Protocol("channel out of range"),
            );
        };
        let Some(tag) = ch.next_publish_tag() else {
            return PublishConfirmFuture::failed(
                self.holding,
                self.channel,
                Error::Protocol("publisher confirms not enabled"),
            );
        };
        h.as_mut().request_flush(target);
        PublishConfirmFuture::confirmed(self.holding, self.channel, tag)
    }

    fn send_fire_forget<M: ClientMethod>(&self, method: &M) -> bool {
        let mut h = self.holding.hold();
        if h.session().shared.fatal.is_some() {
            return false;
        }
        let Some(target) = h.session().shared.ready_conn else {
            return false;
        };
        let Some(slot) = h.as_mut().state_for(target) else {
            return false;
        };
        let mut buf = Owned::with_capacity(32);
        method.write_frame(&mut buf, self.channel);
        if !slot.enqueue(buf.freeze()) {
            return false;
        }
        h.as_mut().request_flush(target);
        true
    }

    fn with_topology<R, F: FnOnce(&mut crate::topology::Topology, u16) -> R>(&self, f: F) -> R {
        let mut h = self.holding.hold();
        let shared = &mut h.as_mut().session_mut().shared;
        f(shared.topology_mut(), self.channel)
    }
}

pub struct ExchangeDeclareOpts<'a> {
    pub exchange: &'a str,
    pub kind: &'a str,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: &'a FieldTable,
}

pub struct QueueDeclareOpts<'a> {
    pub queue: &'a str,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: &'a FieldTable,
}

pub struct ConsumeOpts<'a> {
    pub queue: &'a str,
    pub consumer_tag: &'a str,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub arguments: &'a FieldTable,
}
