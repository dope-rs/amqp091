use super::*;

pub struct QueueDeclareOpts<'a> {
    pub queue: &'a str,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: &'a FieldTable,
}

pub trait QueueOperations<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn queue_declare<'b>(
        &'b self,
        opts: QueueDeclareOpts<'b>,
    ) -> impl Fiber<'d, Output = Result<queue::DeclareOk, Error>> + 'b;
    fn queue_bind<'b>(
        &'b self,
        queue: &'b str,
        exchange: &'b str,
        routing_key: &'b str,
        arguments: &'b FieldTable,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
    fn queue_unbind(
        &self,
        queue_name: &str,
        exchange: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> MethodFuture<'a, 'd, S, E, ()>;
    fn queue_purge(&self, queue_name: &str) -> MethodFuture<'a, 'd, S, E, queue::PurgeOk>;
    fn queue_delete<'b>(
        &'b self,
        queue_name: &'b str,
        if_unused: bool,
        if_empty: bool,
    ) -> impl Fiber<'d, Output = Result<queue::DeleteOk, Error>> + 'b;
}

impl<'a, 'd, S, E> QueueOperations<'a, 'd, S, E> for Channel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn queue_declare<'b>(
        &'b self,
        opts: QueueDeclareOpts<'b>,
    ) -> impl Fiber<'d, Output = Result<queue::DeclareOk, Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }

    fn queue_bind<'b>(
        &'b self,
        queue: &'b str,
        exchange: &'b str,
        routing_key: &'b str,
        arguments: &'b FieldTable,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }

    fn queue_unbind(
        &self,
        queue_name: &str,
        exchange: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> MethodFuture<'a, 'd, S, E, ()> {
        let resolved = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, queue_name))
            .unwrap_or_else(|| queue_name.to_string());
        let frame = self.handle.port.encode(|out| {
            queue::Unbind {
                queue: &resolved,
                exchange,
                routing_key,
                arguments,
            }
            .write_frame(out, self.channel);
        });
        MethodFuture::new(
            self.handle,
            self.channel,
            ExpectMethod::QueueUnbindOk,
            frame,
        )
    }

    fn queue_purge(&self, queue_name: &str) -> MethodFuture<'a, 'd, S, E, queue::PurgeOk> {
        let resolved = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, queue_name))
            .unwrap_or_else(|| queue_name.to_string());
        let frame = self.handle.port.encode(|out| {
            queue::Purge {
                queue: &resolved,
                no_wait: false,
            }
            .write_frame(out, self.channel);
        });
        MethodFuture::new(self.handle, self.channel, ExpectMethod::QueuePurgeOk, frame)
    }

    fn queue_delete<'b>(
        &'b self,
        queue_name: &'b str,
        if_unused: bool,
        if_empty: bool,
    ) -> impl Fiber<'d, Output = Result<queue::DeleteOk, Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }
}
