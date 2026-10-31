use crate::common::amqp::phase::{Open, Opened, Preface, Secure, Start, Started, Tuning};
use crate::common::amqp::{Peer, Tune};
pub mod common;

use std::mem;

use common::amqp::Duplex;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

const CLIENT: [u8; 44] = *b"AMQP\0\0\x09\x01\x01\0\0\0\0\0\x04\0\x0a\0\x0b\xce\x01\0\0\0\0\0\x04\0\x0a\0\x1f\xce\x01\0\0\0\0\0\x04\0\x0a\0\x28\xce";
const SERVER: [u8; 69] = *b"\x01\0\0\0\0\0\x1c\0\x0a\0\x0a\0\x09\0\0\0\0\0\0\0\x05PLAIN\0\0\0\x05en_US\xce\x01\0\0\0\0\0\x0c\0\x0a\0\x1e\0\x10\0\x02\0\0\0\0\xce\x01\0\0\0\0\0\x05\0\x0a\0\x29\0\xce";

#[test]
fn canonical_is_bounded() {
    let mut io = Duplex {
        input: CLIENT,
        output: [0; 69],
        read: 0,
        written: 0,
        read_max: usize::MAX,
        write_max: usize::MAX,
        writes: 0,
        vectors: [0; 3],
    };
    let (result, alloc) = Tracker::usage(|| Peer::new(&mut io).open(Tune::new(16, 131_072, 0)));
    result.expect("canonical handshake");
    assert_eq!(alloc, (0, 0));
    assert_eq!(io.output, SERVER);
    assert_eq!(io.writes, 3);
    assert_eq!(io.vectors, [8, 4, 4]);
}

#[test]
fn preserves_partial_io() {
    let mut io = Duplex {
        input: CLIENT,
        output: [0; 69],
        read: 0,
        written: 0,
        read_max: 1,
        write_max: 3,
        writes: 0,
        vectors: [0; 3],
    };
    Peer::new(&mut io)
        .open(Tune::new(16, 131_072, 0))
        .expect("partial handshake");
    assert_eq!(io.output, SERVER);
    assert!(io.writes > 3);

    let pointer = mem::size_of::<&mut Duplex<44, 69>>();
    assert_eq!(mem::size_of::<Peer<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Preface<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Start<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Started<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Secure<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Tuning<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Open<'static, Duplex<44, 69>>>(), pointer);
    assert_eq!(mem::size_of::<Opened<'static, Duplex<44, 69>>>(), pointer);
}
