use std::num;

use o3::alloc::BoxSliceExt as _;
use o3::buffer::hold;

use crate::failure::{self, wire};
use crate::message::field;
use crate::protocol::wire::text::{long, short};

mod raw;
mod tune;

pub use tune::Tune;

const DEFAULT_TOPOLOGY_IDENTITIES: usize = 8192;
const DEFAULT_TOPOLOGY_RECORDS: usize = 256;

pub struct Config {
    pub(crate) auth: Auth,
    pub(crate) tune: Tune,
    pub(crate) capacity: Capacity,
    pub(crate) properties: field::Table,
}

pub(crate) struct Auth {
    pub(crate) virtual_host: short::Buf,
    pub(crate) response: long::Buf,
    pub(crate) locale: short::Buf,
}

impl Auth {
    pub(crate) fn name(&self) -> short::Short<'_> {
        short::literal!(short::Short::try_new("PLAIN"))
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Frame(num::NonZero<u32>);

#[derive(Clone, Copy)]
pub struct Capacity {
    pub(crate) channels: usize,
    pub(crate) channel: Channel,
    pub(crate) body: Body,
    pub(crate) egress: Egress,
    pub(crate) topology: Topology,
    pub(crate) terminal_causes: usize,
}

#[derive(Clone, Copy)]
pub struct Channel {
    pub(crate) methods: usize,
    pub(crate) confirms: usize,
    pub(crate) consumers: usize,
    pub(crate) deliveries: usize,
    pub(crate) returns: usize,
}

impl Default for Channel {
    fn default() -> Self {
        Self::new(1024, 4096, 256, 1024, 256)
    }
}

#[derive(Clone, Copy)]
pub struct Body {
    pub(crate) max_size: usize,
    pub(crate) buffer: usize,
    pub(crate) fragments: usize,
    pub(crate) retention: hold::Policy,
}

#[derive(Clone, Copy)]
pub struct Egress {
    pub(crate) frames: usize,
    pub(crate) buffer: usize,
}

/// Connection-owned retained method text, topology bytes, entries, and broker identities.
#[derive(Clone, Copy)]
pub struct Topology {
    pub(crate) bytes: usize,
    pub(crate) identities: usize,
    pub(crate) exchanges: usize,
    pub(crate) queues: usize,
    pub(crate) queue_bindings: usize,
    pub(crate) exchange_bindings: usize,
}

impl Default for Frame {
    fn default() -> Self {
        Self(num::NonZero::<u32>::MIN.saturating_add(131_072 - 1))
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::new(16 * 1024 * 1024, 64 * 1024 * 1024, 4096)
    }
}

impl Default for Egress {
    fn default() -> Self {
        Self::new(64, 1024 * 1024)
    }
}

impl Default for Topology {
    fn default() -> Self {
        Self::new(16 * 1024 * 1024)
    }
}

impl Default for Capacity {
    fn default() -> Self {
        Self::new(16, Channel::default(), Body::default(), Egress::default())
    }
}

impl Frame {
    pub const MIN: u32 = 4096;

    pub const fn new(value: u32) -> Result<Self, failure::Error> {
        match num::NonZero::new(value) {
            Some(value) if value.get() >= Self::MIN => Ok(Self(value)),
            _ => Err(failure::Error::InvalidConfig(wire::Config::FrameMinimum {
                actual: value,
                minimum: Self::MIN,
            })),
        }
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Channel {
    pub const fn new(
        methods: usize,
        confirms: usize,
        consumer: usize,
        delivery: usize,
        returns: usize,
    ) -> Self {
        Self {
            methods,
            confirms,
            consumers: consumer,
            deliveries: delivery,
            returns,
        }
    }

    pub const fn methods(mut self, value: usize) -> Self {
        self.methods = value;
        self
    }

    pub const fn confirms(mut self, value: usize) -> Self {
        self.confirms = value;
        self
    }

    pub const fn consumers(mut self, value: usize) -> Self {
        self.consumers = value;
        self
    }

    pub const fn deliveries(mut self, value: usize) -> Self {
        self.deliveries = value;
        self
    }

    pub const fn returns(mut self, value: usize) -> Self {
        self.returns = value;
        self
    }
}

impl Body {
    pub const fn new(max_size: usize, buffer: usize, parts: usize) -> Self {
        Self {
            max_size,
            buffer,
            fragments: parts,
            retention: hold::Policy::new(4096, 2),
        }
    }

    pub const fn max_size(mut self, value: usize) -> Self {
        self.max_size = value;
        self
    }

    pub const fn buffer(mut self, value: usize) -> Self {
        self.buffer = value;
        self
    }

    pub const fn fragments(mut self, value: usize) -> Self {
        self.fragments = value;
        self
    }

    pub const fn retention(mut self, shared: usize, factor: usize) -> Self {
        self.retention = hold::Policy::new(shared, factor);
        self
    }
}

impl Egress {
    pub const fn new(frames: usize, buffer: usize) -> Self {
        Self { frames, buffer }
    }

    pub const fn frames(mut self, value: usize) -> Self {
        self.frames = value;
        self
    }

    pub const fn buffer(mut self, value: usize) -> Self {
        self.buffer = value;
        self
    }
}

impl Topology {
    pub const fn new(bytes: usize) -> Self {
        Self {
            bytes,
            identities: DEFAULT_TOPOLOGY_IDENTITIES,
            exchanges: DEFAULT_TOPOLOGY_RECORDS,
            queues: DEFAULT_TOPOLOGY_RECORDS,
            queue_bindings: DEFAULT_TOPOLOGY_RECORDS,
            exchange_bindings: DEFAULT_TOPOLOGY_RECORDS,
        }
    }

    pub const fn bytes(mut self, value: usize) -> Self {
        self.bytes = value;
        self
    }

    /// Maximum simultaneously retained broker-returned queue names and
    /// consumer tags. Each slot accepts the full AMQP shortstr domain.
    pub const fn identities(mut self, value: usize) -> Self {
        self.identities = value;
        self
    }

    /// Maximum committed or in-flight exchange declarations per connection.
    pub const fn exchanges(mut self, value: usize) -> Self {
        self.exchanges = value;
        self
    }

    /// Maximum committed or in-flight queue declarations per connection.
    pub const fn queues(mut self, value: usize) -> Self {
        self.queues = value;
        self
    }

    /// Maximum committed or in-flight queue bindings per connection.
    pub const fn routes(mut self, value: usize) -> Self {
        self.queue_bindings = value;
        self
    }

    /// Maximum committed or in-flight exchange bindings per connection.
    pub const fn links(mut self, value: usize) -> Self {
        self.exchange_bindings = value;
        self
    }

    pub const fn get(self) -> usize {
        self.bytes
    }

    pub const fn identity_cap(self) -> usize {
        self.identities
    }

    pub const fn exchange_cap(self) -> usize {
        self.exchanges
    }

    pub const fn queue_cap(self) -> usize {
        self.queues
    }

    pub const fn route_cap(self) -> usize {
        self.queue_bindings
    }

    pub const fn link_cap(self) -> usize {
        self.exchange_bindings
    }
}

impl Capacity {
    pub const fn new(channels: usize, channel: Channel, body: Body, egress: Egress) -> Self {
        Self {
            channels,
            channel,
            body,
            egress,
            topology: Topology::new(16 * 1024 * 1024),
            terminal_causes: 2,
        }
    }

    pub const fn channels(mut self, value: usize) -> Self {
        self.channels = value;
        self
    }

    pub const fn channel(mut self, value: Channel) -> Self {
        self.channel = value;
        self
    }

    pub const fn body(mut self, value: Body) -> Self {
        self.body = value;
        self
    }

    pub const fn egress(mut self, value: Egress) -> Self {
        self.egress = value;
        self
    }

    pub const fn topology(mut self, value: Topology) -> Self {
        self.topology = value;
        self
    }

    /// Fixed bound for retained transport causes; each live generation
    /// reserves one cell before admission.
    pub const fn causes(mut self, value: usize) -> Self {
        self.terminal_causes = value;
        self
    }
}

impl Config {
    pub fn new(user: impl AsRef<str>, password: impl AsRef<str>) -> Result<Self, failure::Error> {
        let mut props = field::Table::new();
        for (key, value) in [
            ("product", b"dope-amqp091".as_slice()),
            ("version", env!("CARGO_PKG_VERSION").as_bytes()),
            ("platform", b"dope"),
            ("copyright", b"MIT"),
        ] {
            let value =
                Box::<[u8]>::try_copy(value).map_err(|error| failure::Error::Exhausted {
                    target: wire::Allocation::FieldTable,
                    error,
                })?;
            props.insert(key, field::Value::string(value.into_vec())?)?;
        }
        let mut caps = field::Table::new();
        caps.insert("publisher_confirms", field::Value::from(true))?;
        caps.insert("consumer_cancel_notify", field::Value::from(true))?;
        caps.insert("exchange_exchange_bindings", field::Value::from(true))?;
        caps.insert("basic.nack", field::Value::from(true))?;
        caps.insert("connection.blocked", field::Value::from(true))?;
        caps.insert("authentication_failure_close", field::Value::from(true))?;
        props.insert("capabilities", field::Value::table(caps)?)?;
        Ok(Self {
            auth: Auth {
                virtual_host: short::Buf::new("/")?,
                locale: short::Buf::new("en_US")?,
                // Build the secret last so allocation failure cannot bypass Auth's drop.
                response: long::Buf::try_plain(user.as_ref(), password.as_ref())?,
            },
            tune: Tune::default(),
            capacity: Capacity::default(),
            properties: props,
        })
    }

    pub fn vhost(mut self, vhost: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.try_vhost(vhost)?;
        Ok(self)
    }

    /// Replaces the virtual host after validation and allocation succeed.
    pub fn try_vhost(&mut self, vhost: impl AsRef<str>) -> Result<(), failure::Error> {
        self.auth.virtual_host = short::Buf::resource(vhost, short::Resource::VirtualHost)?;
        Ok(())
    }

    /// Adds or replaces one AMQP `connection.start-ok` client property.
    ///
    /// The value is validated and encoded when the configuration is built, so
    /// connection establishment performs no metadata allocation.
    pub fn property(
        mut self,
        key: impl AsRef<str>,
        value: Vec<u8>,
    ) -> Result<Self, failure::Error> {
        self.try_property(key, value)?;
        Ok(self)
    }

    /// Preserves existing entries on failure; newly reserved capacity may remain.
    pub fn try_property(
        &mut self,
        key: impl AsRef<str>,
        value: Vec<u8>,
    ) -> Result<(), failure::Error> {
        self.properties.insert(key, field::Value::string(value)?)?;
        Ok(())
    }

    pub fn locale(mut self, locale: impl AsRef<str>) -> Result<Self, failure::Error> {
        self.auth.locale = short::Buf::new(locale)?;
        Ok(self)
    }

    pub fn tune(mut self, tune: Tune) -> Self {
        self.tune = tune;
        self
    }

    pub fn capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }
}
