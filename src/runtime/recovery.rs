use std::task::{Context, Poll};

use dope::WakeRef;
use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::transport::Transport;
use o3::buffer::Owned;

use crate::Error;
use crate::method::{ClientMethod, basic, channel, confirm, exchange, queue};
use crate::protocol::{ExpectMethod, Session};
use crate::runtime::handles::{Channel, Connection};
use crate::runtime::handshake::Handshake;
use crate::runtime::method_future::MethodFuture;
use crate::topology::QueueDecl;

pub async fn auto_recover<'d, S, E>(
    holding: Holding<'d, Connector<0, Session, S, E>>,
) -> Result<(), Error>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone> + 'd,
{
    let conn_handle = Connection::from_holding(holding);

    Handshake::new(holding).await?;
    holding
        .hold()
        .as_mut()
        .session_mut()
        .shared
        .enable_auto_recovery();

    loop {
        wait_disconnect(holding).await;
        {
            let session = holding.session();
            if session.shared.intentionally_closed() {
                return Ok(());
            }
            if let Some(err) = session.shared.fatal.as_ref()
                && err.is_fatal_class()
            {
                return Err(err.clone());
            }
        }
        Handshake::new(holding).await?;
        replay_topology(&conn_handle).await?;
    }
}

async fn wait_disconnect<'d, S, E>(holding: Holding<'d, Connector<0, Session, S, E>>)
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    std::future::poll_fn(move |cx: &mut Context<'_>| {
        let mut h = holding.hold();
        let ready = h.session().shared.ready_conn;
        let Some(target) = ready else {
            return Poll::Ready(());
        };
        if h.as_mut().state_for(target).is_none() {
            h.as_mut().session_mut().shared.clear_dead_ready_conn();
            return Poll::Ready(());
        }
        h.as_mut()
            .session_mut()
            .shared
            .register_closed_waker(WakeRef::verified(cx.waker()));
        let ready = h.session().shared.ready_conn;
        match ready {
            None => Poll::Ready(()),
            Some(target) if h.as_mut().state_for(target).is_none() => {
                h.as_mut().session_mut().shared.clear_dead_ready_conn();
                Poll::Ready(())
            }
            Some(_) => Poll::Pending,
        }
    })
    .await
}

async fn replay_topology<'d, S, E>(conn: &Connection<'d, S, E>) -> Result<(), Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    let ch_ids: Vec<u16> = conn
        .holding()
        .session()
        .shared
        .topology()
        .channel_ids()
        .collect();
    for ch_id in ch_ids {
        replay_channel(conn, ch_id).await?;
    }
    Ok(())
}

async fn replay_channel<'d, S, E>(conn: &Connection<'d, S, E>, ch_id: u16) -> Result<(), Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    raw_open_channel(conn.holding(), ch_id).await?;
    let chan = conn.channel(ch_id);

    let (
        confirm_enabled,
        qos_spec,
        exchanges,
        queues,
        queue_bindings,
        exchange_bindings,
        consumers,
    ) = {
        let holding = conn.holding();
        let ct = holding
            .session()
            .shared
            .topology()
            .channel_topo(ch_id)
            .cloned()
            .unwrap_or_default();
        (
            ct.confirm_enabled,
            ct.qos,
            ct.exchanges,
            ct.queues,
            ct.queue_bindings,
            ct.exchange_bindings,
            ct.consumers,
        )
    };

    if confirm_enabled {
        raw_method::<_, _, ()>(
            &chan,
            ExpectMethod::ConfirmSelectOk,
            confirm::Select { no_wait: false },
        )
        .await?;
    }

    if let Some(q) = qos_spec {
        raw_method::<_, _, ()>(
            &chan,
            ExpectMethod::BasicQosOk,
            basic::Qos {
                prefetch_size: q.prefetch_size,
                prefetch_count: q.prefetch_count,
                global: q.global,
            },
        )
        .await?;
    }

    for ex in &exchanges {
        raw_method::<_, _, ()>(
            &chan,
            ExpectMethod::ExchangeDeclareOk,
            exchange::Declare {
                exchange: &ex.name,
                kind: &ex.kind,
                passive: false,
                durable: ex.durable,
                auto_delete: ex.auto_delete,
                internal: ex.internal,
                no_wait: false,
                arguments: &ex.arguments,
            },
        )
        .await?;
    }

    let mut declared: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for q in &queues {
        let new_name = raw_queue_declare(&chan, q).await?;
        update_topology_server_name(&conn.holding(), ch_id, &q.user_name, &new_name);
        declared.insert(q.user_name.clone(), new_name);
    }

    let resolve = |user_name: &str| -> String {
        declared
            .get(user_name)
            .cloned()
            .unwrap_or_else(|| effective_queue_name(&conn.holding(), ch_id, user_name))
    };

    for b in &queue_bindings {
        let queue_name = resolve(&b.queue_user_name);
        raw_method::<_, _, ()>(
            &chan,
            ExpectMethod::QueueBindOk,
            queue::Bind {
                queue: &queue_name,
                exchange: &b.exchange,
                routing_key: &b.routing_key,
                no_wait: false,
                arguments: &b.arguments,
            },
        )
        .await?;
    }

    for b in &exchange_bindings {
        raw_method::<_, _, ()>(
            &chan,
            ExpectMethod::ExchangeBindOk,
            exchange::Bind {
                destination: &b.destination,
                source: &b.source,
                routing_key: &b.routing_key,
                no_wait: false,
                arguments: &b.arguments,
            },
        )
        .await?;
    }

    for c in &consumers {
        let queue_name = resolve(&c.queue_user_name);
        raw_method::<_, _, basic::ConsumeOk>(
            &chan,
            ExpectMethod::BasicConsumeOk,
            basic::Consume {
                queue: &queue_name,
                consumer_tag: &c.consumer_tag,
                no_local: c.no_local,
                no_ack: c.no_ack,
                exclusive: c.exclusive,
                no_wait: false,
                arguments: &c.arguments,
            },
        )
        .await?;
    }
    Ok(())
}

fn effective_queue_name<'d, S, E>(
    holding: &Holding<'d, Connector<0, Session, S, E>>,
    ch_id: u16,
    user_name: &str,
) -> String
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding
        .session()
        .shared
        .topology()
        .resolve_server_queue_view(ch_id, user_name)
        .unwrap_or_else(|| user_name.to_string())
}

fn update_topology_server_name<'d, S, E>(
    holding: &Holding<'d, Connector<0, Session, S, E>>,
    ch_id: u16,
    user_name: &str,
    server_name: &str,
) where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding
        .hold()
        .as_mut()
        .session_mut()
        .shared
        .topology_mut()
        .update_server_name(ch_id, user_name, server_name);
}

async fn raw_open_channel<'d, S, E>(
    holding: Holding<'d, Connector<0, Session, S, E>>,
    ch_id: u16,
) -> Result<(), Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    let mut buf = Owned::with_capacity(16);
    channel::Open.write_frame(&mut buf, ch_id);
    MethodFuture::<S, E, ()>::new(holding, ch_id, ExpectMethod::ChannelOpenOk, buf.freeze()).await
}

async fn raw_queue_declare<'d, S, E>(
    chan: &Channel<'d, S, E>,
    q: &QueueDecl,
) -> Result<String, Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    let ok = raw_method::<_, _, queue::DeclareOk>(
        chan,
        ExpectMethod::QueueDeclareOk,
        queue::Declare {
            queue: &q.user_name,
            passive: false,
            durable: q.durable,
            exclusive: q.exclusive,
            auto_delete: q.auto_delete,
            no_wait: false,
            arguments: &q.arguments,
        },
    )
    .await?;
    Ok(ok.queue)
}

async fn raw_method<'d, S, E, R>(
    chan: &Channel<'d, S, E>,
    expect: ExpectMethod,
    method: impl ClientMethod,
) -> Result<R, Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: crate::runtime::extract::Extract,
{
    let mut buf = Owned::with_capacity(64);
    method.write_frame(&mut buf, chan.id());
    MethodFuture::<S, E, R>::new(chan.holding(), chan.id(), expect, buf.freeze()).await
}
