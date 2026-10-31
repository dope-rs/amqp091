use dope_amqp091::protocol::wire::Write;
use o3::buffer::write::consume;

#[derive(Default)]
pub struct Sink(pub(crate) Vec<u8>);

impl Write for Sink {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn status(&self) -> Result<(), consume::Error> {
        Ok(())
    }

    fn push(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

impl AsMut<[u8]> for Sink {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }
}
