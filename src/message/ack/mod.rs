mod seal;
pub(crate) use seal::Seal;

/// Automatic broker acknowledgement selected at consumer registration.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Auto;

/// Explicit client acknowledgement selected at consumer registration.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Manual;

/// Exactly one AMQP disposition for one received delivery.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Ack,
    Nack { requeue: bool },
    Reject { requeue: bool },
}

/// A statically selected AMQP consumer acknowledgement mode.
pub trait Mode: Seal + Copy {
    #[doc(hidden)]
    const NO_ACK: bool;
}

impl Mode for Auto {
    const NO_ACK: bool = true;
}

impl Mode for Manual {
    const NO_ACK: bool = false;
}
