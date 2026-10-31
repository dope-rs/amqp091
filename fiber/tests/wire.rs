use crate::common::amqp::wire::{Frame, Method};
pub mod common;

use std::io::{Cursor, ErrorKind};
use std::mem;

use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn encodes_one_buffer() {
    let (raw, alloc) =
        Tracker::usage(|| Frame::try_encode(3, 7, b"payload").expect("AMQP frame fixture"));
    assert_eq!(alloc, (1, 15));
    assert_eq!(raw, b"\x03\0\x07\0\0\0\x07payload\xce");

    let (method, alloc) =
        Tracker::usage(|| Method::try_encode(7, 60, 40, b"arguments").expect("AMQP frame fixture"));
    assert_eq!(alloc, (1, 21));
    assert_eq!(method, b"\x01\0\x07\0\0\0\x0d\0\x3c\0\x28arguments\xce");
}

#[test]
fn moves_backing() {
    let encoded = Method::try_encode(7, 60, 40, b"arguments").expect("AMQP frame fixture");
    let mut source = Cursor::new(encoded.as_slice());
    let (frame, alloc) = Tracker::usage(|| Frame::read(&mut source).expect("frame"));
    assert_eq!(alloc, (1, 13));
    let payload = frame.payload().as_ptr();

    let (method, alloc) = Tracker::usage(|| Method::try_from(frame).expect("method"));
    assert_eq!(alloc, (0, 0));
    assert_eq!(method.payload().as_ptr(), payload);
    assert_eq!(method.arguments(), b"arguments");
    let (_, alloc) = Tracker::usage(|| method.check(7, 60, 40).expect("AMQP method route"));
    assert_eq!(alloc, (0, 0));
}

#[test]
fn preserves_layout() {
    assert_eq!(
        mem::size_of::<Frame>(),
        mem::size_of::<(u8, u16, Vec<u8>)>()
    );
    assert_eq!(mem::size_of::<Method>(), mem::size_of::<(u16, Vec<u8>)>());
}

#[test]
fn rejects_boundaries() {
    let mut bad_end = Cursor::new(b"\x01\0\0\0\0\0\0\0");
    assert_eq!(
        (Frame::read(&mut bad_end))
            .err()
            .expect("invalid frame end")
            .kind(),
        ErrorKind::InvalidData
    );

    let wrong = Frame::try_encode(2, 1, &[0, 60, 0, 40]).expect("AMQP frame fixture");
    assert_eq!(
        (Method::read(&mut Cursor::new(wrong)))
            .err()
            .expect("content header is not a method")
            .kind(),
        ErrorKind::InvalidData
    );

    let short = Frame::try_encode(1, 1, &[0, 60, 0]).expect("AMQP frame fixture");
    assert_eq!(
        (Method::read(&mut Cursor::new(short)))
            .err()
            .expect("truncated method route")
            .kind(),
        ErrorKind::InvalidData
    );

    use crate::common::amqp::{Error, wire};
    for (bytes, offset) in [
        (b"".as_slice(), 0),
        (b"\x03ab".as_slice(), 0),
        (b"".as_slice(), usize::MAX),
    ] {
        assert!(matches!(
            wire::short(bytes, offset),
            Err(Error::Truncated { .. })
        ));
    }
    let mut output = vec![9];
    assert!(matches!(
        wire::put_short(&mut output, [0; 256]),
        Err(Error::Length(_))
    ));
    assert_eq!(
        output,
        [9],
        "failed short string admission must preserve output"
    );
    let encoded = Method::try_encode(7, 60, 40, &[]).expect("AMQP method fixture");
    let method = Method::read(&mut Cursor::new(encoded)).expect("AMQP method");
    assert!(matches!(
        method.check(8, 60, 40),
        Err(Error::Route {
            expected: [8, 60, 40],
            actual: [7, 60, 40]
        })
    ));
}
