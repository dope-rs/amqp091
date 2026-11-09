use crate::common::amqp::wire::{Frame, Method};
pub mod common;

use crate::common::amqp::Error;

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
    let mut source = encoded.as_slice();
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
fn rejects_boundaries() {
    let mut bad_end = b"\x01\0\0\0\0\0\0\0".as_slice();
    assert!(matches!(Frame::read(&mut bad_end), Err(Error::FrameEnd(0))));

    let wrong = Frame::try_encode(2, 1, &[0, 60, 0, 40]).expect("AMQP frame fixture");
    assert!(matches!(
        Method::read(&mut wrong.as_slice()),
        Err(Error::FrameType(2))
    ));

    let short = Frame::try_encode(1, 1, &[0, 60, 0]).expect("AMQP frame fixture");
    assert!(matches!(
        Method::read(&mut short.as_slice()),
        Err(Error::MethodLength(3))
    ));

    use crate::common::amqp::wire;
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
    let method = Method::read(&mut encoded.as_slice()).expect("AMQP method");
    assert!(matches!(
        method.check(8, 60, 40),
        Err(Error::Route {
            expected: [8, 60, 40],
            actual: [7, 60, 40]
        })
    ));
}
