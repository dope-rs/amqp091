use super::*;

pub struct ExchangeDeclareOpts<'a> {
    pub exchange: &'a str,
    pub kind: &'a str,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: &'a FieldTable,
}

pub trait ExchangeOperations<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn exchange_declare<'b>(
        &'b self,
        opts: ExchangeDeclareOpts<'b>,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
    fn exchange_delete<'b>(
        &'b self,
        name: &'b str,
        if_unused: bool,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
    fn exchange_bind<'b>(
        &'b self,
        destination: &'b str,
        source: &'b str,
        routing_key: &'b str,
        arguments: &'b FieldTable,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
    fn exchange_unbind(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> MethodFuture<'a, 'd, S, E, ()>;
}

impl<'a, 'd, S, E> ExchangeOperations<'a, 'd, S, E> for Channel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn exchange_declare<'b>(
        &'b self,
        opts: ExchangeDeclareOpts<'b>,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }

    fn exchange_delete<'b>(
        &'b self,
        name: &'b str,
        if_unused: bool,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }

    fn exchange_bind<'b>(
        &'b self,
        destination: &'b str,
        source: &'b str,
        routing_key: &'b str,
        arguments: &'b FieldTable,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
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
        })
    }

    fn exchange_unbind(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> MethodFuture<'a, 'd, S, E, ()> {
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
}
