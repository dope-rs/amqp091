use std::collections::HashMap;
use std::task::Poll;

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::wait::WaitFn;
use dope_net::Transport;

use crate::error::Error;
use crate::method::core::ClientMethod;
use crate::method::{basic, channel, confirm, exchange, queue};
use crate::protocol::ExpectMethod;
use crate::runtime::channel::extract::Extract;
use crate::runtime::channel::method::future::MethodFuture;
use crate::runtime::connection::handshake::Handshake;
use crate::runtime::handles::AmqpHandle;
use crate::runtime::handles::channel::Channel;
use crate::runtime::handles::connection::Connection;
use crate::topology::QueueDecl;

pub struct AutoRecovery<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
}

impl<'a, 'd, S, E> AutoRecovery<'a, 'd, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone> + 'd,
{
    pub fn new(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self { handle }
    }

    pub fn run(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        let handle = self.handle;
        dope_fiber::fiber!('d => async move {
            let connection = Connection::from_handle(handle);

            Handshake::new(handle).await?;
            handle
                .connector()
                .session()
                .with_shared(|shared| shared.enable_auto_recovery());

            loop {
                wait_disconnect(handle).await?;
                let state = handle.connector().session().with_shared(|shared| {
                    (
                        shared.intentionally_closed(),
                        shared
                            .fatal
                            .as_ref()
                            .filter(|error| error.is_fatal_class())
                            .cloned(),
                    )
                });
                if state.0 {
                    return Ok(());
                }
                if let Some(error) = state.1 {
                    return Err(error);
                }
                Handshake::new(handle).await?;
                replay_topology(&connection).await?;
            }
        })
    }
}

#[dope_fiber::fiber_fn('d)]
async fn wait_disconnect<'a, 'd, S, E>(handle: AmqpHandle<'a, 'd, S, E>) -> Result<(), Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    WaitFn::new(move |cx, waiter| {
        let conn = handle.connector();
        let Some(target) = conn.session().with_shared(|shared| shared.ready_conn) else {
            return Poll::Ready(Ok(()));
        };
        if !conn.is_active(target) {
            conn.session()
                .with_shared(|shared| shared.clear_dead_ready_conn());
            return Poll::Ready(Ok(()));
        }
        let target = conn.session().with_shared(|shared| {
            (
                shared.try_register_closed_waker(waiter, cx.as_ref()),
                shared.ready_conn,
            )
        });
        match target {
            (false, _) => Poll::Ready(Err(Error::WaiterCapacity)),
            (_, None) => Poll::Ready(Ok(())),
            (_, Some(target)) if !conn.is_active(target) => {
                conn.session()
                    .with_shared(|shared| shared.clear_dead_ready_conn());
                Poll::Ready(Ok(()))
            }
            (_, Some(_)) => Poll::Pending,
        }
    })
    .await
}

#[dope_fiber::fiber_fn('d)]
async fn replay_topology<'a, 'd, S, E>(conn: &Connection<'a, 'd, S, E>) -> Result<(), Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    let ch_ids: Vec<u16> = conn
        .handle()
        .connector()
        .session()
        .with_shared(|shared| shared.topology().channel_ids().collect());
    for ch_id in ch_ids {
        replay_channel(conn, ch_id).await?;
    }
    Ok(())
}

#[dope_fiber::fiber_fn('d)]
async fn replay_channel<'a, 'd, S, E>(
    conn: &Connection<'a, 'd, S, E>,
    ch_id: u16,
) -> Result<(), Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    let frame = conn
        .handle()
        .encode(|out| channel::Open.write_frame(out, ch_id));
    MethodFuture::<S, E, ()>::new(conn.handle(), ch_id, ExpectMethod::ChannelOpenOk, frame).await?;
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
        let ct = conn.handle().connector().session().with_shared(|shared| {
            shared
                .topology()
                .channel_topo(ch_id)
                .cloned()
                .unwrap_or_default()
        });
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

    let mut declared: HashMap<String, String> = HashMap::new();
    for q in &queues {
        let new_name = raw_queue_declare(&chan, q).await?;
        update_topology_server_name(conn.handle(), ch_id, &q.user_name, &new_name);
        declared.insert(q.user_name.clone(), new_name);
    }

    let resolve = |user_name: &str| -> String {
        declared
            .get(user_name)
            .cloned()
            .unwrap_or_else(|| effective_queue_name(conn.handle(), ch_id, user_name))
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

fn effective_queue_name<'a, 'd, S, E>(
    handle: AmqpHandle<'a, 'd, S, E>,
    ch_id: u16,
    user_name: &str,
) -> String
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle
        .connector()
        .session()
        .with_shared(|shared| {
            shared
                .topology()
                .resolve_server_queue_view(ch_id, user_name)
        })
        .unwrap_or_else(|| user_name.to_string())
}

fn update_topology_server_name<'a, 'd, S, E>(
    handle: AmqpHandle<'a, 'd, S, E>,
    ch_id: u16,
    user_name: &str,
    server_name: &str,
) where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle.connector().session().with_shared(|shared| {
        shared
            .topology_mut()
            .update_server_name(ch_id, user_name, server_name)
    });
}

#[dope_fiber::fiber_fn('d)]
async fn raw_queue_declare<'a, 'd, S, E>(
    chan: &Channel<'a, 'd, S, E>,
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

#[dope_fiber::fiber_fn('d)]
async fn raw_method<'a, 'd, S, E, R>(
    chan: &Channel<'a, 'd, S, E>,
    expect: ExpectMethod,
    method: impl ClientMethod,
) -> Result<R, Error>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    let frame = chan
        .handle()
        .encode(|out| method.write_frame(out, chan.id()));
    MethodFuture::<S, E, R>::new(chan.handle(), chan.id(), expect, frame).await
}
