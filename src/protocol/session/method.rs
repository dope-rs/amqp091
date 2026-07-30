use super::super::*;
use super::content::SessionContent;

pub(in crate::protocol) trait SessionMethod<'d> {
    fn handle_method(
        &mut self,
        head: Frame,
        conn_id: Token,
        state: &mut ConnState<'d>,
        out: &mut Queue<{ IOV_CAP }, Outbound<'d>>,
    ) -> Result<(), Error>;
}

impl<'d> SessionMethod<'d> for Session<'d> {
    fn handle_method(
        &mut self,
        head: Frame,
        conn_id: Token,
        state: &mut ConnState<'d>,
        out: &mut Queue<{ IOV_CAP }, Outbound<'d>>,
    ) -> Result<(), Error> {
        let channel = head.channel;
        let server = Server::parse(head.payload)?;

        if matches!(state.phase, ConnPhase::AwaitingStart)
            && let Server::ConnectionStart(start) = &server
        {
            return self.connection_start(start, state, out);
        }
        if matches!(state.phase, ConnPhase::AwaitingTune)
            && let Server::ConnectionTune(tune) = &server
        {
            return self.connection_tune(tune, state, out);
        }
        if matches!(state.phase, ConnPhase::AwaitingOpenOk)
            && let Server::ConnectionOpenOk(_) = &server
        {
            state.phase = ConnPhase::Ready;
            self.with_shared(|shared| {
                shared.ready_conn = Some(conn_id);
                shared.fatal = None;
                shared.ready_generation = shared.ready_generation.saturating_add(1);
                shared.handshake_wakers.wake();
            });
            return Ok(());
        }

        match server {
            Server::ConnectionStart(_)
            | Server::ConnectionTune(_)
            | Server::ConnectionOpenOk(_) => Err(Error::Protocol(
                "unexpected connection setup method in current phase",
            )),
            Server::ConnectionSecure(_) => Err(Error::Auth(
                "SASL challenge mid-conversation not supported".into(),
            )),
            Server::ConnectionClose(close) => {
                Self::send_method(out, 0, &connection::CloseOk)?;
                state.phase = ConnPhase::Closed;
                state.pending_close = true;
                let recoverable = Error::connection_close_is_recoverable(close.reply_code);
                state.close_permanent = !recoverable;
                let err = Error::Server {
                    reply_code: close.reply_code,
                    reply_text: close.reply_text,
                    class_id: close.class_id,
                    method_id: close.method_id,
                };
                state.for_each_channel_mut(|c| {
                    c.phase = ChannelPhase::Closed;
                    c.clear_inbound();
                    c.fail_all(&err);
                });
                if recoverable {
                    self.with_shared(Shared::record_recoverable_disconnect);
                } else {
                    self.with_shared(|shared| shared.record_failure(err));
                }
                Ok(())
            }
            Server::ConnectionCloseOk(_) => {
                state.phase = ConnPhase::Closed;
                state.pending_close = true;
                state.close_permanent = false;
                self.with_shared(|shared| {
                    shared.ready_conn = None;
                    shared.closed_wakers.wake();
                });
                Ok(())
            }
            Server::ConnectionBlocked(b) => {
                self.with_shared(|shared| {
                    shared.blocked = true;
                    shared.blocked_reason = b.reason;
                });
                Ok(())
            }
            Server::ConnectionUnblocked(_) => {
                self.with_shared(|shared| {
                    shared.blocked = false;
                    shared.blocked_reason.clear();
                    shared.progress_wakers.wake();
                });
                Ok(())
            }
            Server::ChannelOpenOk(_) => {
                state.with_channel_mut(channel, |c| {
                    c.reset_for_reopen();
                    c.phase = ChannelPhase::Open;
                });
                Self::complete(
                    state,
                    channel,
                    ExpectMethod::ChannelOpenOk,
                    PendingResponse::Empty,
                    out,
                )?;
                Ok(())
            }
            Server::ChannelFlow(f) => {
                Self::send_method(out, channel, &channel::FlowOk { active: f.active })?;
                state.with_channel_mut(channel, |c| {
                    c.set_flow_active(f.active);
                });
                if f.active {
                    self.with_shared(|shared| shared.progress_wakers.wake());
                }
                Ok(())
            }
            Server::ChannelFlowOk(_) => Ok(()),
            Server::ChannelClose(close) => {
                Self::send_method(out, channel, &channel::CloseOk)?;
                let err = Error::Server {
                    reply_code: close.reply_code,
                    reply_text: close.reply_text.clone(),
                    class_id: close.class_id,
                    method_id: close.method_id,
                };
                state.with_channel_mut(channel, |c| {
                    c.retire();
                    c.fail_all(&err);
                });
                self.with_shared(|shared| {
                    shared.topology.remove_channel(channel);
                    shared.collect_channel(channel);
                });
                Ok(())
            }
            Server::ChannelCloseOk(_) => {
                state.with_channel_mut(channel, |c| {
                    c.retire();
                });
                let next_frame = state
                    .with_channel_mut(channel, |c| {
                        c.complete_head_with(ExpectMethod::ChannelCloseOk, PendingResponse::Empty)
                    })
                    .transpose()?
                    .flatten();
                if let Some(next_frame) = next_frame {
                    out.try_enqueue(next_frame)
                        .map_err(|_| Error::Other("egress queue full".into()))?;
                }
                self.with_shared(|shared| {
                    shared.topology.remove_channel(channel);
                    shared.collect_channel(channel);
                });
                Ok(())
            }
            Server::ExchangeDeclareOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeDeclareOk,
                PendingResponse::Empty,
                out,
            ),
            Server::ExchangeDeleteOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeDeleteOk,
                PendingResponse::Empty,
                out,
            ),
            Server::ExchangeBindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeBindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::ExchangeUnbindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeUnbindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::QueueDeclareOk(ok) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueDeclareOk,
                PendingResponse::QueueDeclareOk(ok),
                out,
            ),
            Server::QueueBindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueBindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::QueueUnbindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueUnbindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::QueuePurgeOk(ok) => Self::complete(
                state,
                channel,
                ExpectMethod::QueuePurgeOk,
                PendingResponse::QueuePurgeOk(ok),
                out,
            ),
            Server::QueueDeleteOk(ok) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueDeleteOk,
                PendingResponse::QueueDeleteOk(ok),
                out,
            ),
            Server::BasicQosOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::BasicQosOk,
                PendingResponse::Empty,
                out,
            ),
            Server::BasicConsumeOk(ok) => {
                let tag = ok.consumer_tag.clone();
                let key = state
                    .with_channel_mut(channel, |c| c.register_consumer(tag))
                    .ok_or(Error::Protocol("channel out of range"))??;
                Self::complete(
                    state,
                    channel,
                    ExpectMethod::BasicConsumeOk,
                    PendingResponse::BasicConsumeOk(ok, key),
                    out,
                )?;
                self.with_shared(|shared| shared.progress_wakers.wake());
                Ok(())
            }
            Server::BasicCancelOk(ok) => {
                let tag = ok.consumer_tag.clone();
                let res = Self::complete(
                    state,
                    channel,
                    ExpectMethod::BasicCancelOk,
                    PendingResponse::BasicCancelOk(ok),
                    out,
                );
                state
                    .with_channel_mut(channel, |c| c.cancel_consumer(&tag))
                    .ok_or(Error::Protocol("cancel on unknown channel"))??;
                res
            }
            Server::BasicCancel(c_msg) => state
                .with_channel_mut(channel, |c| c.cancel_consumer(&c_msg.consumer_tag))
                .ok_or(Error::Protocol("cancel on unknown channel"))?,
            Server::BasicRecoverOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::BasicRecoverOk,
                PendingResponse::Empty,
                out,
            ),
            Server::BasicAck(a) => state
                .with_channel_mut(channel, |c| {
                    c.confirm_publishes(a.delivery_tag, a.multiple, PublishConfirm::Ack)
                })
                .ok_or(Error::Protocol("confirm on unknown channel"))?,
            Server::BasicNack(n) => state
                .with_channel_mut(channel, |c| {
                    c.confirm_publishes(
                        n.delivery_tag,
                        n.multiple,
                        PublishConfirm::Nack { requeue: n.requeue },
                    )
                })
                .ok_or(Error::Protocol("confirm on unknown channel"))?,
            Server::BasicDeliver(d) => state
                .with_channel_mut(channel, |c| c.begin_content(PendingMethod::Deliver(d)))
                .ok_or(Error::Protocol("delivery on unknown channel"))?,
            Server::BasicReturn(rt) => state
                .with_channel_mut(channel, |c| c.begin_content(PendingMethod::Return(rt)))
                .ok_or(Error::Protocol("return on unknown channel"))?,
            Server::BasicGetOk(_) => Err(Error::Protocol(
                "unsolicited Basic.GetOk (Basic.Get not used)",
            )),
            Server::BasicGetEmpty(_) => Ok(()),
            Server::ConfirmSelectOk(_) => {
                state.with_channel_mut(channel, |c| {
                    c.enable_confirm();
                });
                Self::complete(
                    state,
                    channel,
                    ExpectMethod::ConfirmSelectOk,
                    PendingResponse::Empty,
                    out,
                )
            }
            Server::TxSelectOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::TxSelectOk,
                PendingResponse::Empty,
                out,
            ),
            Server::TxCommitOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::TxCommitOk,
                PendingResponse::Empty,
                out,
            ),
            Server::TxRollbackOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::TxRollbackOk,
                PendingResponse::Empty,
                out,
            ),
        }
    }
}
