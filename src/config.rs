use crate::wire::FieldTable;

#[derive(Debug, Clone)]
pub struct Config {
    pub virtual_host: String,
    pub user: String,
    pub password: String,
    pub mechanism: Mechanism,
    pub locale: String,
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
    pub client_properties: FieldTable,
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
}

impl Config {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        let mut props = FieldTable::new();
        props.insert(
            "product",
            crate::wire::FieldValue::LongStr(b"dope-amqp091".to_vec()),
        );
        props.insert(
            "version",
            crate::wire::FieldValue::LongStr(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        );
        props.insert(
            "platform",
            crate::wire::FieldValue::LongStr(b"dope".to_vec()),
        );
        props.insert(
            "copyright",
            crate::wire::FieldValue::LongStr(b"MIT".to_vec()),
        );
        let mut caps = FieldTable::new();
        caps.insert("publisher_confirms", crate::wire::FieldValue::Bool(true));
        caps.insert(
            "consumer_cancel_notify",
            crate::wire::FieldValue::Bool(true),
        );
        caps.insert(
            "exchange_exchange_bindings",
            crate::wire::FieldValue::Bool(true),
        );
        caps.insert("basic.nack", crate::wire::FieldValue::Bool(true));
        caps.insert("connection.blocked", crate::wire::FieldValue::Bool(true));
        caps.insert(
            "authentication_failure_close",
            crate::wire::FieldValue::Bool(true),
        );
        props.insert("capabilities", crate::wire::FieldValue::Table(caps));
        Self {
            virtual_host: "/".into(),
            user: user.into(),
            password: password.into(),
            mechanism: Mechanism::Plain,
            locale: "en_US".into(),
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
            client_properties: props,
        }
    }

    pub fn with_vhost(mut self, vhost: impl Into<String>) -> Self {
        self.virtual_host = vhost.into();
        self
    }

    pub fn with_heartbeat(mut self, seconds: u16) -> Self {
        self.heartbeat = seconds;
        self
    }

    pub fn with_channel_max(mut self, n: u16) -> Self {
        self.channel_max = n;
        self
    }

    pub fn with_frame_max(mut self, n: u32) -> Self {
        self.frame_max = n;
        self
    }

    pub fn with_mechanism(mut self, m: Mechanism) -> Self {
        self.mechanism = m;
        self
    }
}
