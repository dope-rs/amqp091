use std::collections::BTreeMap;

use crate::wire::FieldTable;

#[derive(Default, Clone)]
pub struct Topology {
    channels: BTreeMap<u16, ChannelTopology>,
}

#[derive(Default, Clone)]
pub(super) struct ChannelTopology {
    pub(super) confirm_enabled: bool,
    pub(super) qos: Option<QosSpec>,
    pub(super) exchanges: Vec<ExchangeDecl>,
    pub(super) queues: Vec<QueueDecl>,
    pub(super) queue_bindings: Vec<QueueBindSpec>,
    pub(super) exchange_bindings: Vec<ExchangeBindSpec>,
    pub(super) consumers: Vec<ConsumerSpec>,
}

#[derive(Clone, Copy)]
pub(super) struct QosSpec {
    pub(super) prefetch_size: u32,
    pub(super) prefetch_count: u16,
    pub(super) global: bool,
}

#[derive(Clone)]
pub(super) struct ExchangeDecl {
    pub(super) name: String,
    pub(super) kind: String,
    pub(super) durable: bool,
    pub(super) auto_delete: bool,
    pub(super) internal: bool,
    pub(super) arguments: FieldTable,
}

#[derive(Clone)]
pub(super) struct QueueDecl {
    pub(super) user_name: String,
    pub(super) server_name: String,
    pub(super) durable: bool,
    pub(super) exclusive: bool,
    pub(super) auto_delete: bool,
    pub(super) arguments: FieldTable,
}

#[derive(Clone)]
pub(super) struct QueueBindSpec {
    pub(super) queue_user_name: String,
    pub(super) exchange: String,
    pub(super) routing_key: String,
    pub(super) arguments: FieldTable,
}

#[derive(Clone)]
pub(super) struct ExchangeBindSpec {
    pub(super) destination: String,
    pub(super) source: String,
    pub(super) routing_key: String,
    pub(super) arguments: FieldTable,
}

#[derive(Clone)]
pub(super) struct ConsumerSpec {
    pub(super) queue_user_name: String,
    pub(super) consumer_tag: String,
    pub(super) no_local: bool,
    pub(super) no_ack: bool,
    pub(super) exclusive: bool,
    pub(super) arguments: FieldTable,
}

impl Topology {
    pub fn channel_ids(&self) -> impl Iterator<Item = u16> + '_ {
        self.channels.keys().copied()
    }

    pub(super) fn channel_mut(&mut self, id: u16) -> &mut ChannelTopology {
        self.channels.entry(id).or_default()
    }

    pub(super) fn channel_topo(&self, id: u16) -> Option<&ChannelTopology> {
        self.channels.get(&id)
    }

    pub(super) fn resolve_server_queue_view(
        &self,
        channel: u16,
        user_name: &str,
    ) -> Option<String> {
        let ct = self.channels.get(&channel)?;
        ct.queues
            .iter()
            .rev()
            .find(|q| q.user_name == user_name)
            .map(|q| q.server_name.clone())
    }

    pub(super) fn canonical_queue_user_name(&self, channel: u16, queue_ref: &str) -> String {
        if let Some(ct) = self.channels.get(&channel) {
            if ct.queues.iter().any(|q| q.user_name == queue_ref) {
                return queue_ref.to_string();
            }
            if let Some(q) = ct.queues.iter().rev().find(|q| q.server_name == queue_ref) {
                return q.user_name.clone();
            }
        }
        queue_ref.to_string()
    }

    pub(super) fn update_server_name(&mut self, channel: u16, user_name: &str, server_name: &str) {
        if let Some(ct) = self.channels.get_mut(&channel)
            && let Some(q) = ct.queues.iter_mut().find(|q| q.user_name == user_name)
        {
            q.server_name = server_name.to_string();
        }
    }

    pub(super) fn record_queue(&mut self, channel: u16, decl: QueueDecl) {
        let ct = self.channel_mut(channel);
        if let Some(existing) = ct.queues.iter_mut().find(|q| q.user_name == decl.user_name) {
            *existing = decl;
        } else {
            ct.queues.push(decl);
        }
    }

    pub(super) fn record_exchange(&mut self, channel: u16, decl: ExchangeDecl) {
        let ct = self.channel_mut(channel);
        if let Some(existing) = ct.exchanges.iter_mut().find(|e| e.name == decl.name) {
            *existing = decl;
        } else {
            ct.exchanges.push(decl);
        }
    }

    pub(super) fn record_queue_bind(&mut self, channel: u16, b: QueueBindSpec) {
        let ct = self.channel_mut(channel);
        if !ct.queue_bindings.iter().any(|existing| {
            existing.queue_user_name == b.queue_user_name
                && existing.exchange == b.exchange
                && existing.routing_key == b.routing_key
        }) {
            ct.queue_bindings.push(b);
        }
    }

    pub(super) fn record_exchange_bind(&mut self, channel: u16, b: ExchangeBindSpec) {
        let ct = self.channel_mut(channel);
        if !ct.exchange_bindings.iter().any(|existing| {
            existing.source == b.source
                && existing.destination == b.destination
                && existing.routing_key == b.routing_key
        }) {
            ct.exchange_bindings.push(b);
        }
    }

    pub(super) fn record_qos(&mut self, channel: u16, qos: QosSpec) {
        self.channel_mut(channel).qos = Some(qos);
    }

    pub(super) fn record_confirm(&mut self, channel: u16) {
        self.channel_mut(channel).confirm_enabled = true;
    }

    pub(super) fn record_consumer(&mut self, channel: u16, spec: ConsumerSpec) {
        let ct = self.channel_mut(channel);
        if let Some(existing) = ct
            .consumers
            .iter_mut()
            .find(|c| c.consumer_tag == spec.consumer_tag)
        {
            *existing = spec;
        } else {
            ct.consumers.push(spec);
        }
    }

    pub(super) fn remove_consumer(&mut self, channel: u16, tag: &str) {
        if let Some(ct) = self.channels.get_mut(&channel) {
            ct.consumers.retain(|c| c.consumer_tag != tag);
        }
    }

    pub(super) fn forget_queue(&mut self, channel: u16, user_name: &str) {
        if let Some(ct) = self.channels.get_mut(&channel) {
            ct.queues.retain(|q| q.user_name != user_name);
            ct.queue_bindings.retain(|b| b.queue_user_name != user_name);
            ct.consumers.retain(|c| c.queue_user_name != user_name);
        }
    }

    pub(super) fn forget_exchange(&mut self, channel: u16, name: &str) {
        if let Some(ct) = self.channels.get_mut(&channel) {
            ct.exchanges.retain(|e| e.name != name);
            ct.queue_bindings.retain(|b| b.exchange != name);
            ct.exchange_bindings
                .retain(|b| b.source != name && b.destination != name);
        }
    }
}
