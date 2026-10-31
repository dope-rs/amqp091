use crate::protocol::wire::text::short;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reply {
    Frame,
    Command,
    Unexpected,
    Channel,
    Syntax,
}

impl Reply {
    const FRAME: short::Short<'static> = short::literal!(short::Short::try_new("frame too large"));
    const COMMAND: short::Short<'static> =
        short::literal!(short::Short::try_new("command invalid"));
    const UNEXPECTED: short::Short<'static> =
        short::literal!(short::Short::try_new("unexpected frame"));
    const CHANNEL: short::Short<'static> = short::literal!(short::Short::try_new("channel error"));
    const SYNTAX: short::Short<'static> = short::literal!(short::Short::try_new("syntax error"));

    pub(crate) const fn code(self) -> u16 {
        match self {
            Self::Frame => 501,
            Self::Syntax => 502,
            Self::Command => 503,
            Self::Unexpected => 505,
            Self::Channel => 504,
        }
    }

    pub(crate) const fn text(&self) -> short::Short<'_> {
        match self {
            Self::Frame => Self::FRAME,
            Self::Command => Self::COMMAND,
            Self::Unexpected => Self::UNEXPECTED,
            Self::Channel => Self::CHANNEL,
            Self::Syntax => Self::SYNTAX,
        }
    }
}
