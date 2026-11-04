use std::{array, num, str};

use dope::core::platform::wake;
use dope_test::harness::peer;

use crate::common::amqp::wire::route::Route;

/// Failure while driving the broker side of an AMQP handshake.
#[derive(Debug)]
pub enum Error {
    Io(peer::Error),
    UnexpectedEof,
    WriteZero,
    ReadOffset,
    WriteOffset,
    WriteCount,
    FrameType(u8),
    FrameEnd(u8),
    MethodLength(usize),
    PayloadRemaining(u32),
    Parts(usize),
    Notify(wake::NotifyError),
    Wait(wake::WaitError),
    Length(num::TryFromIntError),
    Preface([u8; 8]),
    Capacity {
        length: usize,
    },
    Utf8(str::Utf8Error),
    Truncated {
        offset: usize,
        length: usize,
    },
    Payload {
        expected: Vec<u8>,
        actual: Vec<u8>,
    },
    PayloadLength {
        expected: usize,
        actual: usize,
    },
    Frame {
        expected: (u8, u16),
        actual: (u8, u16),
    },
    UnexpectedRead {
        context: String,
        bytes: usize,
    },
    Redial,
    Slice(array::TryFromSliceError),
    Route {
        expected: [u16; 3],
        actual: [u16; 3],
    },
}

impl From<peer::Error> for Error {
    fn from(error: peer::Error) -> Self {
        Self::Io(error)
    }
}

impl Error {
    pub(crate) fn route(expected: Route, actual: Route) -> Self {
        Self::Route {
            expected: [expected.channel(), expected.class(), expected.method()],
            actual: [actual.channel(), actual.class(), actual.method()],
        }
    }
}
