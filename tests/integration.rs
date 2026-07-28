use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::Duration;

mod support;

use dope::driver;
use dope::manifold::connector::session::Connector;
use dope::manifold::connector::source::health::Static;
use dope::manifold::env::Bundle;
use dope::runtime::executor::{Executor, Session as RuntimeSession};
use dope::runtime::profile::Throughput;
use dope_amqp091::config::Config;
use dope_amqp091::content::properties::BasicProperties;
use dope_amqp091::protocol::PublishConfirm;
use dope_amqp091::protocol::session::Session;
use dope_amqp091::protocol::session::port::AmqpPort;
use dope_amqp091::runtime::connection::recovery::AutoRecovery;
use dope_amqp091::runtime::handles::AmqpHandle;
use dope_amqp091::runtime::handles::channel::consume::{BasicOperations, ConsumeOpts};
use dope_amqp091::runtime::handles::channel::exchanges::{ExchangeDeclareOpts, ExchangeOperations};
use dope_amqp091::runtime::handles::channel::publish::PublishOperations;
use dope_amqp091::runtime::handles::channel::queues::{QueueDeclareOpts, QueueOperations};
use dope_amqp091::runtime::handles::connection::Connection;
use dope_amqp091::wire::field::FieldTable;
use dope_fiber::abi::Fiber;
use dope_fiber::abi::pollfn::PollFn;
use dope_fiber::extensions::SessionExt as _;
use dope_fiber::sleep::TimerExt as _;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::cell::BrandCell;
use support::{PinnedFiber, TcpScript, expect_pending, within};

type AmqpEnv = Bundle<Tcp, Identity, Throughput>;
type AmqpConn<'d> = Connector<'d, 0, Session<'d>, Static<Tcp>, AmqpEnv>;
type AmqpClient<'c, 'd> = Connection<'c, 'd, Static<Tcp>, AmqpEnv>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct AmqpDispatcher<'d> {
    #[pin]
    #[manifold]
    amqp: AmqpConn<'d>,
}

struct Rt<'r, 'scope, 'd: 'scope> {
    sess: &'r mut RuntimeSession<'scope, 'd, AmqpPort<'d>>,
    app: Pin<&'r BrandCell<'d, AmqpDispatcher<'d>>>,
    port: &'d AmqpPort<'d>,
}

impl<'r, 'scope, 'd: 'scope> Rt<'r, 'scope, 'd> {
    fn connection(&mut self) -> AmqpClient<'d, 'd> {
        let connector = AmqpDispatcher::amqp_ref(self.app.borrow_pin(self.sess.token()));
        Connection::from_handle(AmqpHandle::from_cell(connector))
    }

    fn block_on<F: Fiber<'d>>(&mut self, fiber: F) -> F::Output {
        self.sess
            .block_on(
                self.app,
                within(self.port.timer(), Duration::from_secs(10), fiber),
            )
            .expect("runtime park")
            .expect("AMQP test deadline")
    }
}

fn with_rt(
    addr: SocketAddr,
    cfg: Config,
    f: impl for<'r, 'scope, 'd> FnOnce(&mut Rt<'r, 'scope, 'd>),
) {
    let exec = Executor::new(driver::Config::for_tcp_profile::<Throughput>(8))
        .expect("create executor")
        .with_storage_factory(AmqpPort::factory(cfg, 1, 4, 16).expect("valid AMQP port config"));
    exec.enter(|mut sess| {
        let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
        let port = sess.storage() as *const AmqpPort<'_>;
        let port = unsafe { &*port };
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(500), backoff);
        let amqp = {
            let mut driver = sess.driver_access();
            AmqpConn::new(Session::new(port), upstreams, 1, &mut driver).expect("AMQP connector")
        };
        let app = pin!(BrandCell::new(AmqpDispatcher { amqp }));
        let mut rt = Rt {
            sess: &mut sess,
            app: app.as_ref(),
            port,
        };
        f(&mut rt);
    });
}

fn spawn_broker(
    script: impl FnOnce(&mut TcpStream) + Send + 'static,
) -> (SocketAddr, TcpScript<()>) {
    let broker = TcpScript::spawn(script).expect("spawn broker");
    (broker.addr(), broker)
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
        Config::new(self.user.clone(), self.password.clone()).vhost(self.vhost.clone())
    }
}

#[dope_fiber::fiber_fn('d)]
async fn wait_ready<'a, 'd>(conn: AmqpClient<'a, 'd>) {
    conn.handshake().await.expect("handshake");
}

fn raw_frame(frame_type: u8, channel: u16, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.push(frame_type);
    frame.extend_from_slice(&channel.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame.push(0xce);
    frame
}

fn method_frame(channel: u16, class: u16, method: u16, args: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + args.len());
    payload.extend_from_slice(&class.to_be_bytes());
    payload.extend_from_slice(&method.to_be_bytes());
    payload.extend_from_slice(args);
    raw_frame(1, channel, &payload)
}

fn put_short(out: &mut Vec<u8>, value: &[u8]) {
    out.push(value.len() as u8);
    out.extend_from_slice(value);
}

fn oversized_return(channel: u16, size: u64) -> Vec<u8> {
    let mut args = Vec::new();
    args.extend_from_slice(&312u16.to_be_bytes());
    put_short(&mut args, b"NO_ROUTE");
    put_short(&mut args, b"");
    put_short(&mut args, b"");
    let mut frames = method_frame(channel, 60, 50, &args);
    let mut header = Vec::with_capacity(14);
    header.extend_from_slice(&60u16.to_be_bytes());
    header.extend_from_slice(&0u16.to_be_bytes());
    header.extend_from_slice(&size.to_be_bytes());
    header.extend_from_slice(&0u16.to_be_bytes());
    frames.extend_from_slice(&raw_frame(2, channel, &header));
    frames
}

fn returned_body(channel: u16, chunks: &[&[u8]]) -> Vec<u8> {
    let mut args = Vec::new();
    args.extend_from_slice(&312u16.to_be_bytes());
    put_short(&mut args, b"NO_ROUTE");
    put_short(&mut args, b"");
    put_short(&mut args, b"");
    let mut frames = method_frame(channel, 60, 50, &args);
    let size = chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>();
    let mut header = Vec::with_capacity(14);
    header.extend_from_slice(&60u16.to_be_bytes());
    header.extend_from_slice(&0u16.to_be_bytes());
    header.extend_from_slice(&size.to_be_bytes());
    header.extend_from_slice(&0u16.to_be_bytes());
    frames.extend_from_slice(&raw_frame(2, channel, &header));
    for chunk in chunks {
        frames.extend_from_slice(&raw_frame(3, channel, chunk));
    }
    frames
}

fn read_frame(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut header = [0u8; 7];
    stream.read_exact(&mut header).expect("frame header");
    assert_eq!(header[0], 1);
    let channel = u16::from_be_bytes([header[1], header[2]]);
    let len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).expect("frame payload");
    let mut end = [0u8; 1];
    stream.read_exact(&mut end).expect("frame end");
    assert_eq!(end[0], 0xce);
    (channel, payload)
}

fn assert_method(frame: &(u16, Vec<u8>), channel: u16, class: u16, method: u16) {
    assert_eq!(frame.0, channel);
    assert_eq!(
        &frame.1[..4],
        &[class.to_be_bytes(), method.to_be_bytes()].concat()
    );
}

fn broker_handshake(stream: &mut TcpStream) {
    broker_handshake_with_channel_max(stream, 16);
}

fn broker_handshake_with_channel_max(stream: &mut TcpStream, channel_max: u16) {
    let mut protocol = [0u8; 8];
    stream.read_exact(&mut protocol).expect("protocol header");
    assert_eq!(&protocol, b"AMQP\0\0\x09\x01");

    let mut start = vec![0, 9, 0, 0, 0, 0];
    start.extend_from_slice(&5u32.to_be_bytes());
    start.extend_from_slice(b"PLAIN");
    start.extend_from_slice(&5u32.to_be_bytes());
    start.extend_from_slice(b"en_US");
    stream
        .write_all(&method_frame(0, 10, 10, &start))
        .expect("start");
    assert_method(&read_frame(stream), 0, 10, 11);

    let mut tune = Vec::with_capacity(8);
    tune.extend_from_slice(&channel_max.to_be_bytes());
    tune.extend_from_slice(&131_072u32.to_be_bytes());
    tune.extend_from_slice(&0u16.to_be_bytes());
    stream
        .write_all(&method_frame(0, 10, 30, &tune))
        .expect("tune");
    assert_method(&read_frame(stream), 0, 10, 31);
    assert_method(&read_frame(stream), 0, 10, 40);
    stream
        .write_all(&method_frame(0, 10, 41, &[0]))
        .expect("open ok");
}

fn close_capacity_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake_with_channel_max(stream, 512);
        for channel in 1..=257 {
            assert_method(&read_frame(stream), channel, 20, 10);
            stream
                .write_all(&method_frame(channel, 20, 11, &[0, 0, 0, 0]))
                .expect("channel open ok");
        }
        for channel in 1..=256 {
            assert_method(&read_frame(stream), channel, 50, 30);
        }
        for channel in 1..=256 {
            stream
                .write_all(&method_frame(channel, 50, 31, &0u32.to_be_bytes()))
                .expect("purge ok");
        }
        assert_method(&read_frame(stream), 257, 50, 30);
        stream
            .write_all(&method_frame(257, 50, 31, &0u32.to_be_bytes()))
            .expect("rollback purge ok");
    })
}

fn queue_name(frame: &(u16, Vec<u8>)) -> &str {
    let len = frame.1[6] as usize;
    std::str::from_utf8(&frame.1[7..7 + len]).unwrap()
}

fn publish_routing_key(frame: &(u16, Vec<u8>)) -> &str {
    let exchange_len = frame.1[6] as usize;
    let routing_len_offset = 7 + exchange_len;
    let routing_len = frame.1[routing_len_offset] as usize;
    std::str::from_utf8(&frame.1[routing_len_offset + 1..routing_len_offset + 1 + routing_len])
        .unwrap()
}

fn scripted_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");
    })
}

fn auto_channel_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("first channel open ok");
        assert_method(&read_frame(stream), 1, 20, 40);
        stream
            .write_all(&method_frame(1, 20, 41, &[]))
            .expect("channel close ok");
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("second channel open ok");
    })
}

fn server_channel_close_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");
        assert_method(&read_frame(stream), 1, 50, 30);
        let mut close = Vec::new();
        close.extend_from_slice(&404u16.to_be_bytes());
        put_short(&mut close, b"closed");
        close.extend_from_slice(&50u16.to_be_bytes());
        close.extend_from_slice(&30u16.to_be_bytes());
        stream
            .write_all(&method_frame(1, 20, 40, &close))
            .expect("server channel close");
        assert_method(&read_frame(stream), 1, 20, 41);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("reused channel open ok");
    })
}

fn method_capacity_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");

        let first = read_frame(stream);
        assert_method(&first, 1, 50, 30);
        assert_eq!(queue_name(&first), "first");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("first purge ok");

        let third = read_frame(stream);
        assert_method(&third, 1, 50, 30);
        assert_eq!(queue_name(&third), "third");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("third purge ok");
    })
}

fn outbound_capacity_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");
        let publish = read_frame(stream);
        assert_method(&publish, 1, 60, 40);
        assert_eq!(publish_routing_key(&publish), "small");
    })
}

fn body_capacity_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");
        let ready = read_frame(stream);
        assert_method(&ready, 1, 50, 30);
        assert_eq!(queue_name(&ready), "ready");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("ready purge ok");
        let trigger = read_frame(stream);
        assert_method(&trigger, 1, 50, 30);
        assert_eq!(queue_name(&trigger), "trigger");
        stream
            .write_all(&oversized_return(1, 9))
            .expect("oversized return");
    })
}

fn aggregate_body_capacity_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        for channel in 1..=2 {
            assert_method(&read_frame(stream), channel, 20, 10);
            stream
                .write_all(&method_frame(channel, 20, 11, &[0, 0, 0, 0]))
                .expect("channel open ok");
        }
        let ready = read_frame(stream);
        assert_method(&ready, 1, 50, 30);
        assert_eq!(queue_name(&ready), "ready");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("ready purge ok");
        let trigger = read_frame(stream);
        assert_method(&trigger, 1, 50, 30);
        assert_eq!(queue_name(&trigger), "trigger");
        stream
            .write_all(&oversized_return(1, 4))
            .expect("first return");
        stream
            .write_all(&oversized_return(2, 4))
            .expect("second return");
    })
}

fn body_fragment_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");
        let ready = read_frame(stream);
        assert_method(&ready, 1, 50, 30);
        assert_eq!(queue_name(&ready), "first-ready");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("first ready purge ok");
        stream
            .write_all(&returned_body(1, &[b"a", b"b"]))
            .expect("first return");
        let barrier = read_frame(stream);
        assert_method(&barrier, 1, 50, 30);
        assert_eq!(queue_name(&barrier), "barrier");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("barrier purge ok");
        stream
            .write_all(&returned_body(1, &[b"c", b"d"]))
            .expect("second return");
        let done = read_frame(stream);
        assert_method(&done, 1, 50, 30);
        assert_eq!(queue_name(&done), "done");
        stream
            .write_all(&method_frame(1, 50, 31, &0u32.to_be_bytes()))
            .expect("done purge ok");
    })
}

fn oversized_frame_header_broker() -> (SocketAddr, TcpScript<()>) {
    spawn_broker(|stream| {
        broker_handshake(stream);
        assert_method(&read_frame(stream), 1, 20, 10);
        stream
            .write_all(&method_frame(1, 20, 11, &[0, 0, 0, 0]))
            .expect("channel open ok");
        let trigger = read_frame(stream);
        assert_method(&trigger, 1, 50, 30);
        assert_eq!(queue_name(&trigger), "trigger");
        let mut header = vec![1, 0, 0];
        header.extend_from_slice(&131_072u32.to_be_bytes());
        stream.write_all(&header).expect("oversized frame header");
    })
}

#[test]
fn channel_reservation_rejects_duplicates_and_releases_on_drop() {
    let (addr, broker) = scripted_broker();
    with_rt(addr, Config::new("guest", "guest"), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let zero = match conn.open(0).await {
                Ok(_) => panic!("channel 0 opened"),
                Err(error) => error,
            };
            assert!(matches!(zero, dope_amqp091::error::Error::Protocol(_)));
            let first = conn.open(1);
            let duplicate = match conn.open(1).await {
                Ok(_) => panic!("duplicate channel opened"),
                Err(error) => error,
            };
            assert!(matches!(duplicate, dope_amqp091::error::Error::ChannelInUse(1)));
            drop(first);
            let channel = conn.open(1).await.expect("open");
            assert_eq!(channel.id(), 1);
            conn.close_now(200, "OK");
        }));
    });
    broker.finish().expect("broker");
}

#[test]
fn auto_channel_reuses_released_id() {
    let (addr, broker) = auto_channel_broker();
    with_rt(addr, Config::new("guest", "guest"), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let first = conn.open_auto_channel().await.expect("first auto channel");
            assert_eq!(first.id(), 1);
            let late = first.queue_purge("late");
            let mut close = PinnedFiber::new(first.close(200, "OK"));
            expect_pending(close.as_pin()).await;
            let late = late.await;
            assert!(matches!(late, Err(dope_amqp091::error::Error::Closed)));
            close.await.expect("close channel");
            let second = conn.open_auto_channel().await.expect("second auto channel");
            assert_eq!(second.id(), 1);
            conn.close_now(200, "OK");
        }));
    });
    broker.finish().expect("broker");
}

#[test]
fn failed_close_allocation_preserves_channel_state() {
    let (addr, broker) = close_capacity_broker();
    with_rt(
        addr,
        Config::new("guest", "guest")
            .channel_max(512)
            .channel_capacity(257)
            .outbound_capacity(256)
            .outbound_buffer_capacity(2048),
        |rt| {
            let conn = rt.connection();
            rt.block_on(dope_fiber::fiber!('_ => async move {
                wait_ready(conn).await;
                let mut channels = Vec::with_capacity(257);
                for channel in 1..=257 {
                    channels.push(
                        conn.open(channel)
                            .await
                            .unwrap_or_else(|error| panic!("open channel {channel}: {error:?}")),
                    );
                }
                let mut purges = channels[..256]
                    .iter()
                    .map(|channel| PinnedFiber::new(channel.queue_purge("fill")))
                    .collect::<Vec<_>>();
                let mut close = PinnedFiber::new(channels[256].close(200, "OK"));
                PollFn::new(|mut cx| {
                    for purge in &mut purges {
                        assert!(Fiber::poll(purge.as_pin(), cx.as_mut()).is_pending());
                    }
                    let error = match Fiber::poll(close.as_pin(), cx) {
                        Poll::Ready(Err(error)) => error,
                        Poll::Ready(Ok(())) => panic!("close unexpectedly allocated"),
                        Poll::Pending => panic!("close ignored exhausted outbound pool"),
                    };
                    assert!(matches!(
                        error,
                        dope_amqp091::error::Error::CapacityOverflow("outbound buffer")
                    ));
                    Poll::Ready(())
                })
                .await;
                for purge in purges {
                    purge.await.expect("purge");
                }
                channels[256]
                    .queue_purge("after")
                    .await
                    .expect("channel remains open");
            }));
        },
    );
    broker.finish().expect("broker");
}

#[test]
fn server_channel_close_retires_after_pending_result() {
    let (addr, broker) = server_channel_close_broker();
    with_rt(addr, Config::new("guest", "guest"), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let channel = conn.open_auto_channel().await.expect("open channel");
            let error = channel.queue_purge("pending").await.unwrap_err();
            assert!(matches!(
                error,
                dope_amqp091::error::Error::Server {
                    reply_code: 404,
                    ..
                }
            ));
            let reopened = conn.open_auto_channel().await.expect("reopen channel");
            assert_eq!(reopened.id(), 1);
            conn.close_now(200, "OK");
        }));
    });
    broker.finish().expect("broker");
}

#[test]
fn dropped_queued_method_releases_capacity_without_sending() {
    let (addr, broker) = method_capacity_broker();
    with_rt(
        addr,
        Config::new("guest", "guest").per_channel(dope_amqp091::config::ChannelCapacity {
            methods: 2,
            ..Default::default()
        }),
        |rt| {
            let conn = rt.connection();
            rt.block_on(dope_fiber::fiber!('_ => async move {
                wait_ready(conn).await;
                let channel = conn.open(1).await.expect("open channel");

                let mut first = PinnedFiber::new(channel.queue_purge("first"));
                expect_pending(first.as_pin()).await;

                let mut second = PinnedFiber::new(channel.queue_purge("second"));
                expect_pending(second.as_pin()).await;

                let overflow = channel.queue_purge("overflow").await.unwrap_err();
                assert!(matches!(
                    overflow,
                    dope_amqp091::error::Error::CapacityOverflow("method")
                ));
                drop(second);

                let mut third = PinnedFiber::new(channel.queue_purge("third"));
                expect_pending(third.as_pin()).await;

                first.await.expect("first purge");
                third.await.expect("third purge");
            }));
        },
    );
    broker.finish().expect("broker");
}

#[test]
fn outbound_pool_rejects_overflow_and_reuses_its_slot() {
    let (addr, broker) = outbound_capacity_broker();
    with_rt(
        addr,
        Config::new("guest", "guest")
            .outbound_capacity(2)
            .outbound_buffer_capacity(1024),
        |rt| {
            let conn = rt.connection();
            let timer = rt.port.timer();
            rt.block_on(dope_fiber::fiber!('_ => async move {
                wait_ready(conn).await;
                let channel = conn.open(1).await.expect("open channel");
                let properties = BasicProperties::default();
                assert!(!channel.basic_publish("", "oversized", false, &properties, &[0; 2048]));
                assert!(channel.basic_publish("", "small", false, &properties, b"x"));
                timer.sleep(Duration::from_millis(20)).await;
            }));
        },
    );
    broker.finish().expect("broker");
}

#[test]
fn declared_body_size_is_rejected_before_allocation() {
    let (addr, broker) = body_capacity_broker();
    with_rt(addr, Config::new("guest", "guest").max_body_size(8), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let channel = conn.open(1).await.expect("open channel");
            let mut returned = PinnedFiber::new(channel.next_return());
            expect_pending(returned.as_pin()).await;
            channel.queue_purge("ready").await.expect("ready purge");
            let mut trigger = PinnedFiber::new(channel.queue_purge("trigger"));
            expect_pending(trigger.as_pin()).await;
            let error = returned.await.unwrap_err();
            assert!(matches!(
                error,
                dope_amqp091::error::Error::MessageTooLarge { size: 9, max: 8 }
            ));
        }));
    });
    broker.finish().expect("broker");
}

#[test]
fn aggregate_body_capacity_is_shared_across_channels() {
    let (addr, broker) = aggregate_body_capacity_broker();
    with_rt(
        addr,
        Config::new("guest", "guest")
            .max_body_size(8)
            .buffer_capacity(6),
        |rt| {
            let conn = rt.connection();
            rt.block_on(dope_fiber::fiber!('_ => async move {
                wait_ready(conn).await;
                let first = conn.open(1).await.expect("first channel");
                conn.open(2).await.expect("second channel");
                let mut returned = PinnedFiber::new(first.next_return());
                expect_pending(returned.as_pin()).await;
                first.queue_purge("ready").await.expect("ready purge");
                let mut trigger = PinnedFiber::new(first.queue_purge("trigger"));
                expect_pending(trigger.as_pin()).await;
                let error = returned.await.unwrap_err();
                assert!(matches!(
                    error,
                    dope_amqp091::error::Error::CapacityOverflow("body buffer")
                ));
            }));
        },
    );
    broker.finish().expect("broker");
}

#[test]
fn body_fragment_pool_is_reused_after_delivery_drop() {
    let (addr, broker) = body_fragment_broker();
    with_rt(
        addr,
        Config::new("guest", "guest").body_fragment_capacity(1),
        |rt| {
            let conn = rt.connection();
            rt.block_on(dope_fiber::fiber!('_ => async move {
                wait_ready(conn).await;
                let channel = conn.open(1).await.expect("open channel");
                let mut first = PinnedFiber::new(channel.next_return());
                expect_pending(first.as_pin()).await;
                channel.queue_purge("first-ready").await.unwrap();
                let first = first.await.unwrap().unwrap();
                assert!(first.body == b"ab"[..]);
                drop(first);
                let mut second = PinnedFiber::new(channel.next_return());
                expect_pending(second.as_pin()).await;
                channel.queue_purge("barrier").await.unwrap();
                let second = second.await.unwrap().unwrap();
                assert!(second.body == b"cd"[..]);
                channel.queue_purge("done").await.unwrap();
            }));
        },
    );
    broker.finish().expect("broker");
}

#[test]
fn oversized_frame_is_rejected_from_its_header() {
    let (addr, broker) = oversized_frame_header_broker();
    with_rt(addr, Config::new("guest", "guest"), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let channel = conn.open(1).await.expect("open channel");
            let error = channel.queue_purge("trigger").await.unwrap_err();
            assert!(matches!(
                error,
                dope_amqp091::error::Error::FrameTooLarge {
                    size: 131_080,
                    max: 131_072
                }
            ));
        }));
    });
    broker.finish().expect("broker");
}

#[test]
fn handshake_then_open() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping: set RABBITMQ_HOST to run");
        return;
    };
    with_rt(env.tcp_addr(), env.amqp_config(), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            conn.open(1).await.expect("open");
            conn.close_now(200, "OK");
        }));
    });
}

#[test]
fn declare_bind_consume_publish_roundtrip() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping: set RABBITMQ_HOST to run");
        return;
    };
    with_rt(env.tcp_addr(), env.amqp_config(), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let chan = conn.open(1).await.expect("open channel");

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
            let props = BasicProperties::builder()
                .content_type("text/plain")
                .build();
            assert!(chan.basic_publish("amq.direct", &q.queue, false, &props, body));

            let delivery = consumer.recv().await.expect("recv").expect("Some delivery");
            assert!(delivery.body == body[..]);
            assert!(chan.basic_ack(delivery.method.delivery_tag, false));

            chan.close(200, "OK").await.expect("channel close");
            conn.close_now(200, "OK");
        }));
    });
}

#[test]
fn publisher_confirms_ack() {
    let Some(env) = EnvCfg::load() else {
        eprintln!("skipping: set RABBITMQ_HOST to run");
        return;
    };
    with_rt(env.tcp_addr(), env.amqp_config(), |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let chan = conn.open(1).await.expect("open channel");
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
        }));
    });
}

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

fn with_deadline<'d, F: Fiber<'d>>(
    fut: F,
    rounds: u64,
) -> impl Fiber<'d, Output = Option<F::Output>> {
    dope_fiber::fiber!('d => async move {
        let mut budget = rounds * 2_000_000;
        let mut fut = Box::pin(fut);
        PollFn::new(move |mut cx| {
            if let Poll::Ready(value) = Fiber::poll(fut.as_mut(), cx.as_mut()) {
                return Poll::Ready(Some(value));
            }
            budget = budget.saturating_sub(1);
            if budget == 0 {
                return Poll::Ready(None);
            }
            cx.wake();
            Poll::Pending
        })
        .await
    })
}

#[dope_fiber::fiber_fn('d)]
async fn publish_reconnect<'a, 'd>(conn: AmqpClient<'a, 'd>, channel_id: u16, body: &[u8]) -> bool {
    const EXCHANGE: &str = "amqp091.reconnect.x";
    const ROUTING_KEY: &str = "amqp091.reconnect.rk";

    match conn.open(channel_id).await {
        Ok(channel) => {
            let published = channel.basic_publish(
                EXCHANGE,
                ROUTING_KEY,
                false,
                &BasicProperties::default(),
                body,
            );
            let _ = channel.close(200, "pub done").await;
            published
        }
        Err(error) => {
            eprintln!("test: publish open({channel_id}) ERR: {error:?}");
            false
        }
    }
}

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
    let cfg = env.amqp_config().heartbeat(2);
    with_rt(env.tcp_addr(), cfg, |rt| {
        let conn = rt.connection();
        rt.block_on(dope_fiber::fiber!('_ => async move {
            wait_ready(conn).await;
            let chan = conn.open(1).await.expect("open channel");

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

            let mut recover = Some(Box::pin(AutoRecovery::new(conn.handle()).run()));
            let mut conn_fatal: Option<Result<(), dope_amqp091::error::Error>> = None;

            let mut pub_ch_id = 20u16;

            macro_rules! recv_one {
                () => {{
                    let mut recv = Box::pin(consumer.recv());
                    let recover = &mut recover;
                    let conn_fatal = &mut conn_fatal;
                    let conn_ref = &conn;
                    PollFn::new(move |mut cx| {
                        if let Some(fut) = recover.as_mut()
                            && let Poll::Ready(r) = Fiber::poll(fut.as_mut(), cx.as_mut())
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
                                    *recover = Some(Box::pin(
                                        AutoRecovery::new(conn_ref.handle()).run(),
                                    ));
                                }
                            }
                        }
                        if let Some(outcome) = conn_fatal.take() {
                            return Poll::Ready(match outcome {
                                Ok(()) => Ok(None),
                                Err(e) => Err(e),
                            });
                        }
                        Fiber::poll(recv.as_mut(), cx)
                    })
                }};
            }

            let mut received = 0usize;
            for i in 0..3u32 {
                let body = format!("pre-{i}");
                let channel_id = pub_ch_id;
                pub_ch_id += 1;
                let published = publish_reconnect(conn, channel_id, body.as_bytes()).await;
                assert!(published, "publish pre");
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

            induce_disconnect();
            let pre_reconnect = received;

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
                        let body = format!("post-{attempts}");
                        let channel_id = pub_ch_id;
                        pub_ch_id += 1;
                        let ok = publish_reconnect(conn, channel_id, body.as_bytes()).await;
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
                    Some(Ok(None)) => {}
                    Some(Err(e)) => {
                        assert!(
                            !e.is_fatal_class(),
                            "supervisor surfaced FATAL on a recoverable disconnect: {e:?}"
                        );
                    }
                    None => {}
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
        }));
    });
}
