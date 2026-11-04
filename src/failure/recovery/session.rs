use dope::core::driver::receive::credit::error::Arm;
use dope::core::driver::result::SubmitCause;
use dope::core::driver::schedule::ready::wake::{fanout, queue, table};
use dope::manifold::app::receive;
use dope::manifold::client::close;
use dope::manifold::client::conn::Transmit;
use dope::net::link::event;
use o3::buffer::hold;
use o3::buffer::resident::staging::reserve;
use o3::buffer::storage::provided::account;

use crate::failure::{self, recovery};
use crate::protocol::wire::text::resident;

pub(crate) enum Session {}

impl Session {
    pub(crate) fn classify_reason(reason: &failure::Reason) -> recovery::Class {
        use o3::buffer::resident::{AllocError, copy};

        match reason {
            failure::Reason::Receive(event::ParkError::Submit(error)) => match error.cause() {
                SubmitCause::Full => recovery::Class::Recoverable,
                SubmitCause::IdentityExhausted => recovery::Class::Fatal,
            },
            failure::Reason::Ingress(cause) => match cause {
                receive::Failure::Hold(
                    hold::Error::Account(
                        account::Cause::Exhausted { .. }
                        | account::Cause::Budget(_)
                        | account::Cause::Retain(_),
                    )
                    | hold::Error::Resident(
                        AllocError::Layout(_) | AllocError::Exhausted(_) | AllocError::Retain(_),
                    )
                    | hold::Error::Copy(copy::Error::Allocation(_)),
                )
                | receive::Failure::Staging(
                    reserve::Error::Resident(
                        AllocError::Layout(_) | AllocError::Exhausted(_) | AllocError::Retain(_),
                    )
                    | reserve::Error::Spill {
                        error:
                            AllocError::Layout(_) | AllocError::Exhausted(_) | AllocError::Retain(_),
                        ..
                    },
                ) => recovery::Class::Recoverable,
                receive::Failure::Hold(
                    hold::Error::Account(account::Cause::Foreign)
                    | hold::Error::Range(_)
                    | hold::Error::Resident(AllocError::Capacity(_) | AllocError::Budget(_))
                    | hold::Error::Copy(copy::Error::Budget(_)),
                )
                | receive::Failure::Staging(
                    reserve::Error::Capacity(_)
                    | reserve::Error::Resident(AllocError::Capacity(_) | AllocError::Budget(_))
                    | reserve::Error::Spill {
                        error: AllocError::Capacity(_) | AllocError::Budget(_),
                        ..
                    },
                )
                | receive::Failure::Input(_)
                | receive::Failure::Accept(_)
                | receive::Failure::FinishCapacity
                | receive::Failure::ParseCapacity
                | receive::Failure::Contract => recovery::Class::Fatal,
            },
            failure::Reason::Local(
                close::LocalOrigin::OperationCancelled
                | close::LocalOrigin::ResourceFailure
                | close::LocalOrigin::ScheduledRotation,
            )
            | failure::Reason::Capacity(close::Capacity::Transport)
            | failure::Reason::EndpointRetired
            | failure::Reason::Transport
            | failure::Reason::ConnectTimeout
            | failure::Reason::InboundTimeout
            | failure::Reason::SendTimeout
            | failure::Reason::LifetimeTimeout
            | failure::Reason::Remote
            | failure::Reason::Receive(
                event::ParkError::Full
                | event::ParkError::Credit(Arm::Stale(_) | Arm::Busy | Arm::Detached)
                | event::ParkError::Register(_),
            ) => recovery::Class::Recoverable,
            failure::Reason::Local(
                close::LocalOrigin::ApiRequest
                | close::LocalOrigin::SideComplete
                | close::LocalOrigin::RuntimeShutdown
                | close::LocalOrigin::Superseded,
            )
            | failure::Reason::Capacity(
                close::Capacity::IngressStaging
                | close::Capacity::IngressRetention
                | close::Capacity::IdentityExhausted,
            )
            | failure::Reason::Protocol
            | failure::Reason::Egress(_)
            | failure::Reason::Receive(
                event::ParkError::Occupied | event::ParkError::Unavailable,
            ) => recovery::Class::Fatal,
        }
    }
}

impl recovery::Policy for Session {
    fn classify<'b, 'd>(
        failure: recovery::Fault<'b, 'd, Self>,
    ) -> (recovery::Class, failure::Error<resident::Short<'b, 'd>>) {
        let error = failure.error();
        let class = match error {
            failure::Error::Disconnected(reason) => Self::classify_reason(reason),
            failure::Error::Transmit(Transmit::Rejected(error)) => match error.into_error().cause()
            {
                SubmitCause::Full => recovery::Class::Recoverable,
                SubmitCause::IdentityExhausted => recovery::Class::Fatal,
            },
            failure::Error::Closed
            | failure::Error::Method(_)
            | failure::Error::MethodIdentity(_)
            | failure::Error::Receive { .. }
            | failure::Error::Transmit(Transmit::Io(_))
            | failure::Error::Retain(_) => recovery::Class::Recoverable,
            failure::Error::Opening(_)
            | failure::Error::Allocation { .. }
            | failure::Error::Exhausted { .. }
            | failure::Error::Polled
            | failure::Error::Storage { .. }
            | failure::Error::Resident { .. }
            | failure::Error::Hold { .. }
            | failure::Error::Input(_)
            | failure::Error::Transmit(Transmit::Invalid(_))
            | failure::Error::Protocol(_)
            | failure::Error::Field(_)
            | failure::Error::Decode(_)
            | failure::Error::QueueName { .. }
            | failure::Error::ConsumerTag { .. }
            | failure::Error::ConsumerCollision { .. }
            | failure::Error::UnexpectedFlowOk { .. }
            | failure::Error::UnsupportedMechanism
            | failure::Error::UnsupportedLocale
            | failure::Error::Version { .. }
            | failure::Error::FrameTooLarge { .. }
            | failure::Error::MessageTooLarge { .. }
            | failure::Error::InvalidConfig(_)
            | failure::Error::Capacity(_)
            | failure::Error::Egress(_)
            | failure::Error::Reserve(_)
            | failure::Error::Encoding(_)
            | failure::Error::UnknownClass(_)
            | failure::Error::UnknownMethod { .. }
            | failure::Error::Frame(_)
            | failure::Error::Queue(queue::Error::Active { .. } | queue::Error::Lane(_))
            | failure::Error::Fanout(fanout::Error::Active)
            | failure::Error::Table(table::Error::Slot(_) | table::Error::Active { .. })
            | failure::Error::Cancellation(_)
            | failure::Error::WaitRegister(_)
            | failure::Error::WaitRoute(_)
            | failure::Error::WaitCancel(_) => recovery::Class::Fatal,
            failure::Error::Server { .. } if error.is_refused() || error.is_denied() => {
                recovery::Class::Fatal
            }
            failure::Error::Server { .. }
            | failure::Error::ConnectionBlocked(_)
            | failure::Error::ChannelFlowPaused
            | failure::Error::ChannelInUse(_)
            | failure::Error::ChannelCapacity { .. }
            | failure::Error::Queue(queue::Error::Draining { .. } | queue::Error::Full { .. })
            | failure::Error::Fanout(fanout::Error::Full)
            | failure::Error::Table(
                table::Error::Draining { .. } | table::Error::Occupied { .. },
            )
            | failure::Error::ServerNamedQueueNotExclusive => recovery::Class::Recoverable,
        };
        (class, failure.into_error())
    }
}
