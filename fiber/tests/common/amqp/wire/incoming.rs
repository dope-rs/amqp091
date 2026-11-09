use crate::common::amqp::Error;
use crate::common::amqp::io::Read;

use crate::common::amqp::wire::input::Input;
use crate::common::amqp::wire::route::Route;

/// One streaming AMQP method whose payload remains tied to its input.
pub(crate) struct Incoming<'a, IO> {
    route: Route,
    input: Input<'a, IO>,
}

impl<'a, IO: Read> Incoming<'a, IO> {
    pub(crate) fn read(source: &'a mut IO) -> Result<Self, Error> {
        let (header, mut input) = Input::read(source)?;
        if header.frame_type() != super::METHOD {
            return Err(Error::FrameType(header.frame_type()));
        }
        if header.length() < 4 {
            return Err(Error::MethodLength(header.length() as usize));
        }
        let mut bytes = [0; 4];
        input.read_exact(&mut bytes)?;
        let route = Route::new(
            header.channel(),
            u16::from_be_bytes([bytes[0], bytes[1]]),
            u16::from_be_bytes([bytes[2], bytes[3]]),
        );
        Ok(Self { route, input })
    }

    pub(crate) const fn route(&self) -> Route {
        self.route
    }

    pub(crate) fn discard(self) -> Result<(), Error> {
        self.input.discard()
    }

    pub(crate) fn compare(self, expected: &[&[u8]]) -> Result<(), Error> {
        self.input.compare(expected)
    }
}
