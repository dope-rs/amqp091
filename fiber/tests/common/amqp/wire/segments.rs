use crate::common::amqp::{Error, io::Write};

pub(super) fn write<const N: usize>(
    mut parts: [&[u8]; N],
    sink: &mut impl Write,
) -> Result<(), Error> {
    let mut first = 0;
    while first < N {
        let mut written = sink.write_vectored(&parts[first..])?;
        if written == 0 {
            return Err(Error::WriteZero);
        }
        while first < N {
            let length = parts[first].len();
            if written < length {
                parts[first] = &parts[first][written..];
                break;
            }
            written -= length;
            first += 1;
            if written == 0 {
                break;
            }
        }
    }
    Ok(())
}
