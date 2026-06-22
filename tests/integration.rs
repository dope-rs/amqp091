use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverCfg, DriverConfig, Executor};
use dope_amqp091::wire::FieldTable;
use dope_amqp091::{
    BasicProperties, Config, Connection, ConsumeOpts, ExchangeDeclareOpts, PublishConfirm,
    QueueDeclareOpts, Session, auto_recover,
};

type AmqpEnv = Bundle<Tcp, Identity, Throughput>;
type AmqpConn = Connector<0, Session, Static<Tcp>, AmqpEnv>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct AmqpDispatcher {
    #[pin]
    #[manifold]
    amqp: AmqpConn,
}

struct Rt {
    exec: Box<Executor>,
    dispatcher: Pin<Box<AmqpDispatcher>>,
}

impl Rt {
    fn new(addr: SocketAddr, cfg: Config) -> std::io::Result<Self> {
        let mut exec = Box::new(Executor::new(DriverCfg::for_tcp_profile::<Throughput>(8))?);
        let driver = exec.driver_mut();
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(500));
        let dispatcher = Box::pin(AmqpDispatcher {
            amqp: AmqpConn::new(Session::new(cfg), upstreams, 1, driver),
        });
        Ok(Self { exec, dispatcher })
    }

    fn connection<'a>(&mut self) -> Connection<'a, Static<Tcp>, AmqpEnv> {
        let dispatcher = unsafe { Pin::get_unchecked_mut(self.dispatcher.as_mut()) };
        let amqp = &mut dispatcher.amqp as *mut AmqpConn;
        let holding = Holding::of(unsafe { Pin::new_unchecked(&mut *amqp) });
        Connection::from_holding(holding)
    }

    fn block_on<F: Future>(&mut self, fut: F) -> F::Output {
        dope_extra::block_on(
            &mut self.exec,
            self.dispatcher.as_mut(),
            dope::fiber::Fiber::new(fut),
        )
    }
}

struct EnvCfg {
    host: String,
    port: u16,
    user: String,
    password: String,
    vhost: String,
}

impl EnvCfg {
    fn load() -> Option<Self> {
        let host = std::env::var("RABBITMQ_HOST").ok()?;
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Some(Self {
            host,
            port: get("RABBITMQ_PORT", "5672").parse().ok()?,
            user: get("RABBITMQ_USER", "guest"),
            password: get("RABBITMQ_PASSWORD", "guest"),
            vhost: get("RABBITMQ_VHOST", "/"),
        })
    }

    fn tcp_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("AMQP addr")
    }

    fn amqp_config(&self) -> Config {
        Config::new(self.user.clone(), self.password.clone()).with_vhost(self.vhost.clone())
    }
}

async fn wait_ready(conn: Connection<'_, Static<Tcp>, AmqpEnv>) {
    conn.handshake().await.expect("handshake");
    std::future::poll_fn(move |_cx| {
        if conn.is_failed() {
            panic!("amqp091: connection failed");
        }
        if conn.is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}

#[test]
fn handshake_then_open_channel() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping: set RABBITMQ_HOST to run");
        return;
    };
    let mut rt = Rt::new(env.tcp_addr(), env.amqp_config()).expect("rt");
    let conn = rt.connection();
    rt.block_on(async move {
        wait_ready(conn.clone()).await;
        conn.open_channel(1).await.expect("open_channel");
        conn.close_now(200, "OK");
    });
}

#[test]
fn declare_bind_consume_publish_roundtrip() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping: set RABBITMQ_HOST to run");
        return;
    };
    let mut rt = Rt::new(env.tcp_addr(), env.amqp_config()).expect("rt");
    let conn = rt.connection();
    rt.block_on(async move {
        wait_ready(conn.clone()).await;
        let chan = conn.open_and_channel(1).await.expect("open channel");

        let empty = FieldTable::new();
        let q = chan
            .queue_declare(QueueDeclareOpts {
                queue: "",
                passive: false,
                durable: false,
                exclusive: true,
                auto_delete: true,
                arguments: &empty,
            })
            .await
            .expect("queue_declare");

        chan.queue_bind(&q.queue, "amq.direct", &q.queue, &empty)
            .await
            .expect("queue_bind");

        chan.basic_qos(0, 1, false).await.expect("basic_qos");

        let mut consumer = chan
            .basic_consume(ConsumeOpts {
                queue: &q.queue,
                consumer_tag: "test-tag",
                no_local: false,
                no_ack: false,
                exclusive: true,
                arguments: &empty,
            })
            .await
            .expect("basic_consume");

        let body = b"hello-from-test";
        let props = BasicProperties {
            content_type: Some("text/plain".into()),
            ..Default::default()
        };
        assert!(chan.basic_publish("amq.direct", &q.queue, false, &props, body));

        let delivery = consumer.recv().await.expect("recv").expect("Some delivery");
        assert_eq!(delivery.body, body);
        assert!(chan.basic_ack(delivery.method.delivery_tag, false));

        chan.close(200, "OK").await.expect("channel close");
        conn.close_now(200, "OK");
    });
}

#[test]
fn publisher_confirms_ack() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping: set RABBITMQ_HOST to run");
        return;
    };
    let mut rt = Rt::new(env.tcp_addr(), env.amqp_config()).expect("rt");
    let conn = rt.connection();
    rt.block_on(async move {
        wait_ready(conn.clone()).await;
        let chan = conn.open_and_channel(1).await.expect("open channel");
        let empty = FieldTable::new();
        let q = chan
            .queue_declare(QueueDeclareOpts {
                queue: "",
                passive: false,
                durable: false,
                exclusive: true,
                auto_delete: true,
                arguments: &empty,
            })
            .await
            .expect("queue_declare");
        chan.queue_bind(&q.queue, "amq.direct", &q.queue, &empty)
            .await
            .expect("queue_bind");
        chan.confirm_select().await.expect("confirm_select");
        let props = BasicProperties::default();
        let outcome = chan
            .basic_publish_confirm("amq.direct", &q.queue, false, &props, b"confirm-me")
            .await
            .expect("publish confirm");
        assert_eq!(outcome, PublishConfirm::Ack);
        chan.close(200, "OK").await.expect("channel close");
        conn.close_now(200, "OK");
    });
}

/// Induce a REAL broker-initiated disconnect against the local container.
///
///   - default / `FEED_TEST_DISCONNECT=kill`: `docker kill -s KILL` + `docker
///     start` → a pure TCP drop (the connector re-dials; `disconnect()` path).
///   - `FEED_TEST_DISCONNECT=close`: `rabbitmqctl close_all_connections` → a
///     broker `connection.close` (320 CONNECTION_FORCED). This is the path the
///     recovery fix targets: 320 must be classified RECOVERABLE so `auto_recover`
///     reconnects + replays instead of exiting on the lingering `fatal`.
fn induce_disconnect() {
    let container = std::env::var("FEED_TEST_RMQ_CONTAINER").unwrap_or_else(|_| "rmq".into());
    let mode = std::env::var("FEED_TEST_DISCONNECT").unwrap_or_else(|_| "kill".into());
    let out = if mode == "close" {
        std::process::Command::new("docker")
            .args([
                "exec",
                &container,
                "rabbitmqctl",
                "close_all_connections",
                "amqp091-test-induced",
            ])
            .output()
    } else {
        let kill = std::process::Command::new("docker")
            .args(["kill", "-s", "KILL", &container])
            .output()
            .expect("docker kill");
        eprintln!(
            "test: docker kill status={} stderr={}",
            kill.status,
            String::from_utf8_lossy(&kill.stderr)
        );
        std::process::Command::new("docker")
            .args(["start", &container])
            .output()
    };
    let out = out.expect("docker disconnect command");
    eprintln!(
        "test: induce_disconnect(mode={mode}) status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Race a future against a bounded busy-yield budget so the test can't hang.
/// (This dispatcher has no Timer manifold, so we bound by executor progress.)
async fn with_deadline<F: Future>(fut: F, rounds: u64) -> Option<F::Output> {
    let mut budget = rounds * 2_000_000;
    let mut fut = Box::pin(fut);
    std::future::poll_fn(move |cx| {
        if let Poll::Ready(v) = fut.as_mut().poll(cx) {
            return Poll::Ready(Some(v));
        }
        budget = budget.saturating_sub(1);
        if budget == 0 {
            return Poll::Ready(None);
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

/// LOAD-BEARING: after a broker disconnect (TCP kill or 320 forced close) the
/// `auto_recover` supervisor must reconnect AND `replay_topology` must restore
/// delivery so NEW messages published post-reconnect reach the SAME `Consumer`
/// handle. Gated on a local broker (`RABBITMQ_HOST`) + an explicit opt-in
/// (`AMQP_TEST_RECONNECT=1`) because it kills/restarts the container.
#[test]
fn reconnect_resumes_delivery() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping reconnect_resumes_delivery: set RABBITMQ_HOST to run");
        return;
    };
    if std::env::var("AMQP_TEST_RECONNECT").is_err() {
        eprintln!("skipping reconnect_resumes_delivery: set AMQP_TEST_RECONNECT=1 (kills broker)");
        return;
    }
    // Fast heartbeat so a TCP drop is noticed quickly.
    let cfg = env.amqp_config().with_heartbeat(2);
    let mut rt = Rt::new(env.tcp_addr(), cfg).expect("rt");
    let conn = rt.connection();
    rt.block_on(async move {
        wait_ready(conn.clone()).await;
        let chan = conn.open_and_channel(1).await.expect("open channel");

        // Durable topic exchange (survives the connection drop) + ephemeral
        // server-named queue bound by a STABLE routing key, consumed under a
        // STABLE tag — exactly the feed's topology. On reconnect the queue is
        // re-declared under a NEW server name, re-bound by the same key, and
        // re-consumed under the same tag, so a post-reconnect publish routes to
        // the re-bound queue and reaches the same handle.
        let empty = FieldTable::new();
        const EX: &str = "amqp091.reconnect.x";
        const RK: &str = "amqp091.reconnect.rk";
        const TAG: &str = "amqp091-reconnect-tag";
        chan.exchange_declare(ExchangeDeclareOpts {
            exchange: EX,
            kind: "topic",
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: &empty,
        })
        .await
        .expect("exchange declare");
        let q = chan
            .queue_declare(QueueDeclareOpts {
                queue: "",
                passive: false,
                durable: false,
                exclusive: true,
                auto_delete: true,
                arguments: &empty,
            })
            .await
            .expect("queue declare");
        chan.queue_bind(&q.queue, EX, RK, &empty)
            .await
            .expect("queue bind");
        chan.basic_qos(0, 50, false).await.expect("qos");
        let mut consumer = chan
            .basic_consume(ConsumeOpts {
                queue: &q.queue,
                consumer_tag: TAG,
                no_local: false,
                no_ack: false,
                exclusive: true,
                arguments: &empty,
            })
            .await
            .expect("consume");

        // The reconnect supervisor, driven concurrently with recv (as the feed
        // does). Re-armed on a recoverable completion.
        type BgFut<'a> = Pin<Box<dyn Future<Output = Result<(), dope_amqp091::Error>> + 'a>>;
        let mut recover: Option<BgFut> = Some(Box::pin(auto_recover(conn.holding())));
        let mut conn_fatal: Option<Result<(), dope_amqp091::Error>> = None;

        // Publish on a fresh channel each time so a publish never races the
        // consumer channel's replay re-open.
        let mut pub_ch_id = 20u16;
        macro_rules! publish {
            ($body:expr) => {{
                let id = pub_ch_id;
                pub_ch_id += 1;
                match conn.open_and_channel(id).await {
                    Ok(pch) => {
                        let ok =
                            pch.basic_publish(EX, RK, false, &BasicProperties::default(), $body);
                        let _ = pch.close(200, "pub done").await;
                        ok
                    }
                    Err(e) => {
                        eprintln!("test: publish open_and_channel({id}) ERR: {e:?}");
                        false
                    }
                }
            }};
        }

        macro_rules! recv_one {
            () => {{
                let mut recv = Box::pin(consumer.recv());
                let recover = &mut recover;
                let conn_fatal = &mut conn_fatal;
                let conn_ref = &conn;
                std::future::poll_fn(move |cx| {
                    if let Some(fut) = recover.as_mut()
                        && let Poll::Ready(r) = fut.as_mut().poll(cx)
                    {
                        match r {
                            Ok(()) => {
                                *conn_fatal = Some(Ok(()));
                                *recover = None;
                            }
                            Err(e) if e.is_fatal_class() => {
                                *conn_fatal = Some(Err(e));
                                *recover = None;
                            }
                            Err(_) => {
                                *recover = Some(Box::pin(auto_recover(conn_ref.holding())));
                            }
                        }
                    }
                    if let Some(outcome) = conn_fatal.take() {
                        return Poll::Ready(match outcome {
                            Ok(()) => Ok(None),
                            Err(e) => Err(e),
                        });
                    }
                    recv.as_mut().poll(cx)
                })
            }};
        }

        // --- Phase 1: deliver BEFORE the disconnect ---
        let mut received = 0usize;
        for i in 0..3u32 {
            assert!(publish!(format!("pre-{i}").as_bytes()), "publish pre");
        }
        for _ in 0..3 {
            match with_deadline(recv_one!(), 30).await {
                Some(Ok(Some(d))) => {
                    received += 1;
                    chan.basic_ack(d.method.delivery_tag, false);
                }
                Some(Ok(None)) => panic!("consumer closed before disconnect"),
                Some(Err(e)) => panic!("recv error before disconnect: {e:?}"),
                None => panic!("timed out waiting for pre-disconnect message"),
            }
        }
        assert_eq!(received, 3, "received all pre-disconnect messages");
        eprintln!("test: phase 1 OK — received {received} pre-disconnect");

        // --- Induce a REAL broker disconnect ---
        induce_disconnect();
        let pre_reconnect = received;

        // --- Phase 2: the supervisor must reconnect (not fatal-exit on a 320),
        // replay the topology, and DELIVER a post-reconnect message to the SAME
        // consumer handle. ---
        let mut attempts = 0;
        let mut recovered = false;
        let mut settle = 0;
        let resumed = loop {
            attempts += 1;
            assert!(
                attempts < 400,
                "no post-reconnect delivery after {attempts} tries"
            );
            if conn.is_ready() && !recovered {
                recovered = true;
                eprintln!("test: connection RECOVERED to ready (supervisor survived)");
            }
            if recovered {
                settle += 1;
                if settle >= 3 {
                    let ok = publish!(format!("post-{attempts}").as_bytes());
                    if settle % 20 == 3 {
                        eprintln!("test: post-reconnect publish attempt={attempts} ok={ok}");
                    }
                }
            }
            match with_deadline(recv_one!(), 8).await {
                Some(Ok(Some(d))) => {
                    chan.basic_ack(d.method.delivery_tag, false);
                    if d.body.starts_with(b"post-") {
                        received += 1;
                        if received > pre_reconnect {
                            break true;
                        }
                    }
                }
                Some(Ok(None)) => { /* transient: keep going */ }
                Some(Err(e)) => {
                    assert!(
                        !e.is_fatal_class(),
                        "supervisor surfaced FATAL on a recoverable disconnect: {e:?}"
                    );
                }
                None => { /* parked this round; keep trying */ }
            }
        };

        assert!(recovered, "connection never recovered to ready");
        assert!(
            resumed && received > pre_reconnect,
            "delivery did NOT resume after reconnect (replay_topology gap)"
        );
        eprintln!(
            "test: phase 2 OK — reconnected + replayed + delivery RESUMED (total={received})"
        );
        conn.close_now(200, "done");
    });
}
