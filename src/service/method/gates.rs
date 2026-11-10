use std::mem;

use crate::failure::{self, wire};
use crate::protocol::method::{self, conn};
use crate::state::channel::methods::MethodKey;
use crate::state::conn::phase::Phase;
use crate::state::{channel, channels};

pub(in crate::service) struct Channel<'d, const ID: u8> {
    lease: channels::Lease<'d, ID>,
    published: u64,
}

pub(super) fn connection_phase(phase: Phase) -> wire::ConnPhase {
    match phase {
        Phase::NeedsHello => wire::ConnPhase::Hello,
        Phase::Start => wire::ConnPhase::Start,
        Phase::Tune => wire::ConnPhase::Tune,
        Phase::Open => wire::ConnPhase::Open,
        Phase::Ready => wire::ConnPhase::Ready,
    }
}

pub(super) fn connection(
    kind: conn::Kind,
    phase: Phase,
    receipt: Phase,
    closing: bool,
) -> Result<(), failure::Error> {
    let valid = match kind {
        conn::Kind::Start => phase == Phase::Start,
        conn::Kind::Secure | conn::Kind::Tune => phase == Phase::Tune,
        conn::Kind::Open => phase == Phase::Open,
        conn::Kind::Close => matches!(
            phase,
            Phase::Start | Phase::Tune | Phase::Open | Phase::Ready
        ),
        conn::Kind::CloseOk => phase == Phase::Ready && closing,
        conn::Kind::Blocked | conn::Kind::Unblocked => phase == Phase::Ready,
    };
    if valid
        && (!matches!(
            kind,
            conn::Kind::Start | conn::Kind::Secure | conn::Kind::Tune | conn::Kind::Open
        ) || receipt == phase)
    {
        return Ok(());
    }
    let phase = if valid { receipt } else { phase };
    let method = match kind {
        conn::Kind::Start => wire::ConnMethod::Start,
        conn::Kind::Secure => wire::ConnMethod::Secure,
        conn::Kind::Tune => wire::ConnMethod::Tune,
        conn::Kind::Open => wire::ConnMethod::Open,
        conn::Kind::Close => wire::ConnMethod::Close,
        conn::Kind::CloseOk => wire::ConnMethod::CloseOk,
        conn::Kind::Blocked => wire::ConnMethod::Blocked,
        conn::Kind::Unblocked => wire::ConnMethod::Unblocked,
    };
    Err(failure::Error::Protocol(wire::Fault::ConnMethod {
        method,
        phase: connection_phase(phase),
        closing,
    }))
}

impl<'d, const ID: u8> Channel<'d, ID> {
    fn fault(
        kind: method::Kind,
        phase: channel::Phase,
        lease: channels::Lease<'d, ID>,
    ) -> failure::Error {
        let phase = match phase {
            channel::Phase::Closed => wire::ChanPhase::Closed,
            channel::Phase::Opening => wire::ChanPhase::Opening,
            channel::Phase::Open => wire::ChanPhase::Open,
            channel::Phase::Closing => wire::ChanPhase::Closing,
        };
        failure::Error::Protocol(wire::Fault::ChanMethod {
            channel: lease.id(),
            method: kind,
            phase,
        })
    }

    pub(super) fn check(
        kind: method::Kind,
        phase: channel::Phase,
        pending: Option<(MethodKey, method::Kind)>,
        current: Option<MethodKey>,
        lease: channels::Lease<'d, ID>,
        published: u64,
    ) -> Result<Self, failure::Error> {
        let admitted = Self { lease, published };
        match kind {
            method::Kind::Open => {
                if phase != channel::Phase::Opening {
                    return Err(Self::fault(kind, phase, lease));
                }
            }
            method::Kind::CloseOk => {
                if phase != channel::Phase::Closing {
                    return Err(Self::fault(kind, phase, lease));
                }
            }
            method::Kind::Close => {
                return if matches!(
                    phase,
                    channel::Phase::Opening | channel::Phase::Open | channel::Phase::Closing
                ) {
                    Ok(admitted)
                } else {
                    Err(Self::fault(kind, phase, lease))
                };
            }
            method::Kind::Flow
            | method::Kind::Cancel
            | method::Kind::Return
            | method::Kind::Deliver
            | method::Kind::Ack
            | method::Kind::Nack => {
                return if phase == channel::Phase::Open {
                    Ok(admitted)
                } else {
                    Err(Self::fault(kind, phase, lease))
                };
            }
            method::Kind::FlowOk | method::Kind::Get | method::Kind::GetEmpty => {
                return Err(failure::Error::Protocol(wire::Fault::ChanResponse {
                    channel: lease.id(),
                    expected: None,
                    actual: kind,
                }));
            }
            method::Kind::ExchangeDeclare
            | method::Kind::ExchangeDelete
            | method::Kind::ExchangeBind
            | method::Kind::ExchangeUnbind
            | method::Kind::QueueDeclare
            | method::Kind::QueueBind
            | method::Kind::QueueUnbind
            | method::Kind::QueuePurge
            | method::Kind::QueueDelete
            | method::Kind::Qos
            | method::Kind::Consume
            | method::Kind::CancelOk
            | method::Kind::Recover
            | method::Kind::Confirm
            | method::Kind::TxSelect
            | method::Kind::TxCommit
            | method::Kind::TxRollback => {
                if !matches!(phase, channel::Phase::Open | channel::Phase::Closing) {
                    return Err(Self::fault(kind, phase, lease));
                }
            }
        }
        match pending {
            Some((owner, expected)) if expected == kind => {
                if current != Some(owner) {
                    return Err(failure::Error::Protocol(wire::Fault::MethodOrder {
                        expected: owner.into(),
                        actual: current.map(Into::into),
                    }));
                }
                Ok(admitted)
            }
            expected => Err(failure::Error::Protocol(wire::Fault::ChanResponse {
                channel: lease.id(),
                expected: expected.map(|(_, kind)| kind),
                actual: kind,
            })),
        }
    }

    pub(super) fn into_parts(self) -> (channels::Lease<'d, ID>, u64) {
        (self.lease, self.published)
    }
}

const _: () = assert!(
    mem::size_of::<Channel<'static, 0>>()
        == mem::size_of::<channels::Lease<'static, 0>>() + mem::size_of::<u64>()
);
