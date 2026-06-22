#[derive(Debug, Clone)]
pub enum Error {
    Io {
        kind: std::io::ErrorKind,
        message: String,
    },
    Closed,
    Auth(String),
    Protocol(&'static str),
    ProtocolOwned(String),
    Server {
        reply_code: u16,
        reply_text: String,
        class_id: u16,
        method_id: u16,
    },
    UnsupportedMechanism,
    FrameTooLarge {
        size: u32,
        max: u32,
    },
    UnknownClass(u16),
    UnknownMethod {
        class_id: u16,
        method_id: u16,
    },
    ConnectionBlocked(String),
    ChannelFlowPaused,
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { kind, message } => write!(f, "io error: {kind}: {message}"),
            Self::Closed => f.write_str("connection closed"),
            Self::Auth(s) => write!(f, "auth: {s}"),
            Self::Protocol(s) => write!(f, "protocol: {s}"),
            Self::ProtocolOwned(s) => write!(f, "protocol: {s}"),
            Self::Server {
                reply_code,
                reply_text,
                class_id,
                method_id,
            } => write!(
                f,
                "server {reply_code} {reply_text} (class {class_id} method {method_id})"
            ),
            Self::UnsupportedMechanism => f.write_str("no SASL mechanism in common"),
            Self::FrameTooLarge { size, max } => {
                write!(f, "frame {size} exceeds negotiated max {max}")
            }
            Self::UnknownClass(c) => write!(f, "unknown class id {c}"),
            Self::UnknownMethod {
                class_id,
                method_id,
            } => {
                write!(f, "unknown method {method_id} in class {class_id}")
            }
            Self::ConnectionBlocked(reason) => write!(f, "connection blocked: {reason}"),
            Self::ChannelFlowPaused => f.write_str("channel paused by server (flow=false)"),
            Self::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

pub struct ReplyCode;

impl ReplyCode {
    pub const CONTENT_TOO_LARGE: u16 = 311;
    pub const NO_CONSUMERS: u16 = 313;
    pub const CONNECTION_FORCED: u16 = 320;
    pub const CONNECTION_ERROR: u16 = 302;
    pub const INVALID_PATH: u16 = 402;
    pub const ACCESS_REFUSED: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    pub const RESOURCE_LOCKED: u16 = 405;
    pub const PRECONDITION_FAILED: u16 = 406;
    pub const FRAME_ERROR: u16 = 501;
    pub const SYNTAX_ERROR: u16 = 502;
    pub const COMMAND_INVALID: u16 = 503;
    pub const CHANNEL_ERROR: u16 = 504;
    pub const UNEXPECTED_FRAME: u16 = 505;
    pub const RESOURCE_ERROR: u16 = 506;
    pub const NOT_ALLOWED: u16 = 530;
    pub const NOT_IMPLEMENTED: u16 = 540;
    pub const INTERNAL_ERROR: u16 = 541;
}

impl Error {
    pub fn server_code(&self) -> Option<u16> {
        match self {
            Self::Server { reply_code, .. } => Some(*reply_code),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.server_code() == Some(ReplyCode::NOT_FOUND)
    }

    pub fn is_access_refused(&self) -> bool {
        self.server_code() == Some(ReplyCode::ACCESS_REFUSED)
    }

    pub fn is_precondition_failed(&self) -> bool {
        self.server_code() == Some(ReplyCode::PRECONDITION_FAILED)
    }

    pub fn is_resource_locked(&self) -> bool {
        self.server_code() == Some(ReplyCode::RESOURCE_LOCKED)
    }

    pub fn is_not_allowed(&self) -> bool {
        self.server_code() == Some(ReplyCode::NOT_ALLOWED)
    }

    pub fn is_not_implemented(&self) -> bool {
        self.server_code() == Some(ReplyCode::NOT_IMPLEMENTED)
    }

    pub fn is_internal_error(&self) -> bool {
        self.server_code() == Some(ReplyCode::INTERNAL_ERROR)
    }

    pub fn is_resource_error(&self) -> bool {
        self.server_code() == Some(ReplyCode::RESOURCE_ERROR)
    }

    pub fn is_channel_error(&self) -> bool {
        self.server_code() == Some(ReplyCode::CHANNEL_ERROR)
    }

    pub fn is_fatal_class(&self) -> bool {
        matches!(
            self,
            Self::Auth(_) | Self::UnsupportedMechanism | Self::Protocol(_) | Self::ProtocolOwned(_)
        ) || matches!(
            self.server_code(),
            Some(ReplyCode::ACCESS_REFUSED) | Some(ReplyCode::NOT_ALLOWED)
        )
    }

    pub fn connection_close_is_recoverable(reply_code: u16) -> bool {
        matches!(
            reply_code,
            ReplyCode::CONNECTION_FORCED | ReplyCode::CONNECTION_ERROR
        )
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io {
            kind: e.kind(),
            message: e.to_string(),
        }
    }
}
