use super::super::*;

pub(in crate::protocol) trait SessionContent<'d> {
    fn complete(
        state: &mut ConnState<'d>,
        channel: u16,
        expect: ExpectMethod,
        response: PendingResponse,
        out: &mut Queue<{ IOV_CAP }, Outbound>,
    ) -> Result<(), Error>;

    fn connection_start(
        &mut self,
        start: &connection::Start,
        state: &mut ConnState<'d>,
        out: &mut Queue<{ IOV_CAP }, Outbound>,
    ) -> Result<(), Error>;

    fn connection_tune(
        &mut self,
        tune: &connection::Tune,
        state: &mut ConnState<'d>,
        out: &mut Queue<{ IOV_CAP }, Outbound>,
    ) -> Result<(), Error>;

    fn pick(client: u16, server: u16) -> u16;
    fn pick_u32(client: u32, server: u32) -> u32;
    fn handle_header(&mut self, head: Frame, state: &mut ConnState<'d>) -> Result<(), Error>;
    fn handle_body(&mut self, head: Frame, state: &mut ConnState<'d>) -> Result<(), Error>;
    fn finalize_message(
        channel: &mut ChannelState<'d>,
        method: PendingMethod,
        header: ContentHeader,
        body: Body<'static>,
    ) -> Result<(), Error>;
    fn fail_session(&mut self, state: &mut ConnState<'d>, err: Error, permanent: bool);
}

impl<'d> SessionContent<'d> for Session<'d> {
    fn complete(
        state: &mut ConnState<'d>,
        channel: u16,
        expect: ExpectMethod,
        response: PendingResponse,
        out: &mut Queue<{ IOV_CAP }, Outbound>,
    ) -> Result<(), Error> {
        let Some(next_frame) =
            state.with_channel_mut(channel, |c| c.complete_head_with(expect, response))
        else {
            return Ok(());
        };
        if let Some(next_frame) = next_frame? {
            out.try_enqueue(next_frame)
                .map_err(|_| Error::Other("egress queue full".into()))?;
        }
        Ok(())
    }

    fn connection_start(
        &mut self,
        start: &connection::Start,
        state: &mut ConnState<'d>,
        out: &mut Queue<{ IOV_CAP }, Outbound>,
    ) -> Result<(), Error> {
        let mech_name = self.config.mechanism.name();
        if !start
            .mechanisms
            .split(' ')
            .any(|m| !m.is_empty() && m == mech_name)
        {
            return Err(Error::UnsupportedMechanism);
        }
        let response = self
            .config
            .mechanism
            .response(&self.config.user, &self.config.password);
        let locale = if start
            .locales
            .split(' ')
            .any(|l| !l.is_empty() && l == self.config.locale)
        {
            self.config.locale.as_str()
        } else {
            start.locales.split(' ').next().unwrap_or("en_US")
        };
        Self::send_method(
            out,
            0,
            &StartOk {
                client_properties: &self.config.client_properties,
                mechanism: mech_name,
                response: &response,
                locale,
            },
        )?;
        state.phase = ConnPhase::AwaitingTune;
        Ok(())
    }

    fn connection_tune(
        &mut self,
        tune: &connection::Tune,
        state: &mut ConnState<'d>,
        out: &mut Queue<{ IOV_CAP }, Outbound>,
    ) -> Result<(), Error> {
        let channel_max = Self::pick(self.config.channel_max, tune.channel_max);
        let frame_max = Self::pick_u32(self.config.frame_max, tune.frame_max);
        let heartbeat = Self::pick(self.config.heartbeat, tune.heartbeat);
        state.negotiated = Negotiated {
            channel_max,
            frame_max,
            heartbeat,
        };
        self.codec.set_max_frame(frame_max);
        self.with_shared(|shared| shared.negotiated = state.negotiated);
        Self::send_method(
            out,
            0,
            &TuneOk {
                channel_max,
                frame_max,
                heartbeat,
            },
        )?;
        Self::send_method(
            out,
            0,
            &Open {
                virtual_host: &self.config.virtual_host,
            },
        )?;
        state.phase = ConnPhase::AwaitingOpenOk;
        Ok(())
    }

    fn pick(client: u16, server: u16) -> u16 {
        match (client, server) {
            (0, s) => s,
            (c, 0) => c,
            (c, s) => c.min(s),
        }
    }

    fn pick_u32(client: u32, server: u32) -> u32 {
        match (client, server) {
            (0, s) => s,
            (c, 0) => c,
            (c, s) => c.min(s),
        }
    }

    fn handle_header(&mut self, head: Frame, state: &mut ConnState<'d>) -> Result<(), Error> {
        let channel = head.channel;
        let header = ContentHeader::read(head.payload)?;
        state
            .with_channel_mut(channel, |c| {
                let prev = replace(&mut c.inbound, InboundContent::Idle);
                let InboundContent::AwaitingHeader(method) = prev else {
                    return Err(Error::Protocol("HEADER frame without preceding METHOD"));
                };
                if header.body_size > c.limits.body as u64 {
                    return Err(Error::MessageTooLarge {
                        size: header.body_size,
                        max: c.limits.body,
                    });
                }
                if header.body_size == 0 {
                    let body = c.body();
                    Self::finalize_message(c, method, header, body)
                } else {
                    let Some(budget) = c.budget.try_acquire(header.body_size as usize) else {
                        return Err(Error::CapacityOverflow("body buffer"));
                    };
                    let mut body = c.body();
                    body.reserve(budget);
                    c.inbound = InboundContent::AwaitingBody {
                        method,
                        header,
                        body,
                    };
                    Ok(())
                }
            })
            .ok_or(Error::Protocol("HEADER frame on unknown channel"))?
    }

    fn handle_body(&mut self, head: Frame, state: &mut ConnState<'d>) -> Result<(), Error> {
        let channel = head.channel;
        state
            .with_channel_mut(channel, |c| {
                let prev = replace(&mut c.inbound, InboundContent::Idle);
                let InboundContent::AwaitingBody {
                    method,
                    header,
                    mut body,
                } = prev
                else {
                    return Err(Error::Protocol("BODY frame without preceding HEADER"));
                };
                let Some(have) = body.len().checked_add(head.payload.len()) else {
                    return Err(Error::MessageTooLarge {
                        size: u64::MAX,
                        max: c.limits.body,
                    });
                };
                if have as u64 > header.body_size {
                    return Err(Error::Protocol("content body exceeds declared body-size"));
                }
                if body.push(head.payload).is_err() {
                    return Err(Error::CapacityOverflow("body fragment"));
                }
                if have as u64 == header.body_size {
                    Self::finalize_message(c, method, header, body)
                } else {
                    c.inbound = InboundContent::AwaitingBody {
                        method,
                        header,
                        body,
                    };
                    Ok(())
                }
            })
            .ok_or(Error::Protocol("BODY frame on unknown channel"))?
    }

    fn finalize_message(
        channel: &mut ChannelState<'d>,
        method: PendingMethod,
        header: ContentHeader,
        body: Body<'static>,
    ) -> Result<(), Error> {
        match method {
            PendingMethod::Deliver(d) => {
                let delivery = Delivery {
                    method: d,
                    properties: header.properties,
                    body,
                };
                channel.route_delivery(delivery)?;
            }
            PendingMethod::Return(rt) => {
                let msg = Returned {
                    method: rt,
                    properties: header.properties,
                    body,
                };
                channel.route_return(msg)?;
            }
        }
        Ok(())
    }

    fn fail_session(&mut self, state: &mut ConnState<'d>, err: Error, permanent: bool) {
        state.phase = ConnPhase::Closed;
        state.pending_close = true;
        state.close_permanent = permanent;
        state.for_each_channel_mut(|c| {
            c.phase = ChannelPhase::Closed;
            c.clear_inbound();
            c.fail_all(&err);
        });
        self.with_shared(|shared| shared.record_failure(err));
    }
}
