use crate::failure::wire::turn;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::state::channel::consumer) enum Status {
    Pending,
    Active,
    Untracked,
    Recovering,
    Replaying,
    Canceled,
    Retiring,
}

impl Status {
    pub(in crate::state::channel::consumer) const fn phase(self) -> turn::Consumer {
        match self {
            Self::Pending => turn::Consumer::Pending,
            Self::Active => turn::Consumer::Active,
            Self::Untracked => turn::Consumer::Untracked,
            Self::Recovering => turn::Consumer::Recovering,
            Self::Replaying => turn::Consumer::Replaying,
            Self::Canceled => turn::Consumer::Canceled,
            Self::Retiring => turn::Consumer::Retiring,
        }
    }
}
