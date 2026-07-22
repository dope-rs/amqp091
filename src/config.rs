use crate::wire::field::{FieldTable, FieldValue};

#[derive(Debug, Clone)]
pub struct Config {
    pub virtual_host: String,
    pub user: String,
    pub password: String,
    pub mechanism: Mechanism,
    pub locale: String,
    pub channel_max: u16,
    pub channel_capacity: usize,
    pub per_channel: ChannelCapacity,
    pub frame_max: u32,
    pub heartbeat: u16,
    pub max_body_size: usize,
    pub buffer_capacity: usize,
    pub body_fragment_capacity: usize,
    pub outbound_capacity: usize,
    pub outbound_buffer_capacity: usize,
    pub client_properties: FieldTable,
}

#[derive(Debug, Clone, Copy)]
pub struct ChannelCapacity {
    pub methods: usize,
    pub confirms: usize,
    pub consumers: usize,
    pub deliveries: usize,
    pub returns: usize,
}

impl Default for ChannelCapacity {
    fn default() -> Self {
        Self {
            methods: 1024,
            confirms: 4096,
            consumers: 256,
            deliveries: 1024,
            returns: 256,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Mechanism {
    Plain,
    External { identity: String },
}

impl Mechanism {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::External { .. } => "EXTERNAL",
        }
    }

    pub(crate) fn response(&self, user: &str, password: &str) -> Vec<u8> {
        match self {
            Self::Plain => {
                let mut response = Vec::with_capacity(2 + user.len() + password.len());
                response.push(0);
                response.extend_from_slice(user.as_bytes());
                response.push(0);
                response.extend_from_slice(password.as_bytes());
                response
            }
            Self::External { identity } => identity.as_bytes().to_vec(),
        }
    }
}

impl Config {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        let mut props = FieldTable::new();
        props.insert("product", FieldValue::LongStr(b"dope-amqp091".to_vec()));
        props.insert(
            "version",
            FieldValue::LongStr(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        );
        props.insert("platform", FieldValue::LongStr(b"dope".to_vec()));
        props.insert("copyright", FieldValue::LongStr(b"MIT".to_vec()));
        let mut caps = FieldTable::new();
        caps.insert("publisher_confirms", FieldValue::Bool(true));
        caps.insert("consumer_cancel_notify", FieldValue::Bool(true));
        caps.insert("exchange_exchange_bindings", FieldValue::Bool(true));
        caps.insert("basic.nack", FieldValue::Bool(true));
        caps.insert("connection.blocked", FieldValue::Bool(true));
        caps.insert("authentication_failure_close", FieldValue::Bool(true));
        props.insert("capabilities", FieldValue::Table(caps));
        Self {
            virtual_host: "/".into(),
            user: user.into(),
            password: password.into(),
            mechanism: Mechanism::Plain,
            locale: "en_US".into(),
            channel_max: 2047,
            channel_capacity: 16,
            per_channel: ChannelCapacity::default(),
            frame_max: 131_072,
            heartbeat: 60,
            max_body_size: 16 * 1024 * 1024,
            buffer_capacity: 64 * 1024 * 1024,
            body_fragment_capacity: 4096,
            outbound_capacity: 64,
            outbound_buffer_capacity: 1024 * 1024,
            client_properties: props,
        }
    }

    pub fn vhost(mut self, vhost: impl Into<String>) -> Self {
        self.virtual_host = vhost.into();
        self
    }

    pub fn heartbeat(mut self, heartbeat: u16) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    pub fn channel_max(mut self, channel_max: u16) -> Self {
        self.channel_max = channel_max;
        self
    }

    pub fn channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }

    pub fn frame_max(mut self, frame_max: u32) -> Self {
        self.frame_max = frame_max;
        self
    }

    pub fn mechanism(mut self, mechanism: Mechanism) -> Self {
        self.mechanism = mechanism;
        self
    }

    pub fn per_channel(mut self, capacity: ChannelCapacity) -> Self {
        self.per_channel = capacity;
        self
    }

    pub fn max_body_size(mut self, max_body_size: usize) -> Self {
        self.max_body_size = max_body_size;
        self
    }

    pub fn buffer_capacity(mut self, capacity: usize) -> Self {
        self.buffer_capacity = capacity;
        self
    }

    pub fn body_fragment_capacity(mut self, capacity: usize) -> Self {
        self.body_fragment_capacity = capacity;
        self
    }

    pub fn outbound_capacity(mut self, capacity: usize) -> Self {
        self.outbound_capacity = capacity;
        self
    }

    pub fn outbound_buffer_capacity(mut self, capacity: usize) -> Self {
        self.outbound_buffer_capacity = capacity;
        self
    }
}
