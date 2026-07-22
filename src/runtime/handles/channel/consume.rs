use super::*;

pub struct ConsumeOpts<'a> {
    pub queue: &'a str,
    pub consumer_tag: &'a str,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub arguments: &'a FieldTable,
}

pub trait BasicOperations<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn basic_qos<'b>(
        &'b self,
        prefetch_size: u32,
        prefetch_count: u16,
        global: bool,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
    fn basic_consume<'b>(
        &'b self,
        opts: ConsumeOpts<'b>,
    ) -> impl Fiber<'d, Output = Result<Consumer<'a, 'd, S, E>, Error>> + 'b;
    fn basic_cancel<'b>(
        &'b self,
        consumer_tag: &'b str,
    ) -> impl Fiber<'d, Output = Result<basic::CancelOk, Error>> + 'b;
    fn basic_ack(&self, delivery_tag: u64, multiple: bool) -> bool;
    fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> bool;
    fn basic_reject(&self, delivery_tag: u64, requeue: bool) -> bool;
    fn basic_recover(&self, requeue: bool) -> MethodFuture<'a, 'd, S, E, ()>;
    fn confirm_select<'b>(&'b self) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
    fn tx_select(&self) -> MethodFuture<'a, 'd, S, E, ()>;
    fn tx_commit(&self) -> MethodFuture<'a, 'd, S, E, ()>;
    fn tx_rollback(&self) -> MethodFuture<'a, 'd, S, E, ()>;
}

impl<'a, 'd, S, E> BasicOperations<'a, 'd, S, E> for Channel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn basic_qos<'b>(
        &'b self,
        prefetch_size: u32,
        prefetch_count: u16,
        global: bool,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }

    fn basic_consume<'b>(
        &'b self,
        opts: ConsumeOpts<'b>,
    ) -> impl Fiber<'d, Output = Result<Consumer<'a, 'd, S, E>, Error>> + 'b {
        dope_fiber::fiber!('d => async move {
            let user_name = self.with_topology(|t, ch| t.canonical_queue_user_name(ch, opts.queue));
        let resolved_queue = self
            .with_topology(|t, ch| t.resolve_server_queue_view(ch, opts.queue))
            .unwrap_or_else(|| opts.queue.to_string());
        let no_local = opts.no_local;
        let no_ack = opts.no_ack;
        let exclusive = opts.exclusive;
        let args = opts.arguments.clone();
        let fut = self.send_for::<_, RegisteredConsumer>(
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
        let registered = fut.await?;
        let ok = registered.method;
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
            Ok(Consumer::new(
                self.handle,
                self.channel,
                registered.key,
                ok.consumer_tag,
            ))
        })
    }

    fn basic_cancel<'b>(
        &'b self,
        consumer_tag: &'b str,
    ) -> impl Fiber<'d, Output = Result<basic::CancelOk, Error>> + 'b {
        dope_fiber::fiber!('d => async move {
            let fut = self.send_for::<_, basic::CancelOk>(
            ExpectMethod::BasicCancelOk,
            basic::CancelRef {
                consumer_tag,
                no_wait: false,
            },
        );
        let ok = fut.await?;
        self.with_topology(|t, ch| t.remove_consumer(ch, consumer_tag));
            Ok(ok)
        })
    }

    fn basic_ack(&self, delivery_tag: u64, multiple: bool) -> bool {
        self.send_fire_forget(&basic::Ack {
            delivery_tag,
            multiple,
        })
    }

    fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> bool {
        self.send_fire_forget(&basic::Nack {
            delivery_tag,
            multiple,
            requeue,
        })
    }

    fn basic_reject(&self, delivery_tag: u64, requeue: bool) -> bool {
        self.send_fire_forget(&basic::Reject {
            delivery_tag,
            requeue,
        })
    }

    fn basic_recover(&self, requeue: bool) -> MethodFuture<'a, 'd, S, E, ()> {
        self.send_for(ExpectMethod::BasicRecoverOk, basic::Recover { requeue })
    }

    fn confirm_select<'b>(&'b self) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
            let fut = self.send_for::<_, ()>(
            ExpectMethod::ConfirmSelectOk,
            confirm::Select { no_wait: false },
        );
        fut.await?;
        self.with_topology(|t, ch| t.record_confirm(ch));
            Ok(())
        })
    }

    fn tx_select(&self) -> MethodFuture<'a, 'd, S, E, ()> {
        self.send_for(ExpectMethod::TxSelectOk, tx::Select)
    }

    fn tx_commit(&self) -> MethodFuture<'a, 'd, S, E, ()> {
        self.send_for(ExpectMethod::TxCommitOk, tx::Commit)
    }

    fn tx_rollback(&self) -> MethodFuture<'a, 'd, S, E, ()> {
        self.send_for(ExpectMethod::TxRollbackOk, tx::Rollback)
    }
}
