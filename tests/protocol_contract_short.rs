use dope_amqp091::failure;
use dope_amqp091::failure::wire;
use dope_amqp091::protocol::wire::text::short::{self, Buf, Resource, Short};

#[test]
fn rejects_nul() {
    let fault = wire::Fault::TextNul {
        kind: wire::Encoding::Short,
        offset: 5,
    };
    std::assert_matches!(
        Short::new("queue\0suffix").err().expect("short text rejects NUL"),
        failure::Error::Protocol(found) if found == fault
    );
    std::assert_matches!(
        Buf::new("queue\0suffix").expect_err("owned text rejects NUL"),
        failure::Error::Protocol(found) if found == fault
    );
    let fault = wire::Fault::TextNul {
        kind: wire::Encoding::Queue,
        offset: 5,
    };
    std::assert_matches!(
        Short::resource("queue\0suffix", Resource::Queue).err().expect("queue text rejects NUL"),
        failure::Error::Protocol(found) if found == fault
    );
    std::assert_matches!(
        Buf::resource("queue\0suffix", Resource::Queue).expect_err("owned queue rejects NUL"),
        failure::Error::Protocol(found) if found == fault
    );
    assert_eq!(
        short::literal!(Short::try_new("queue")).as_str(),
        Short::new("queue").expect("valid short text").as_str(),
    );
    assert!(Buf::new("queue").is_ok());
}

#[test]
fn enforces_byte_limit() {
    let mut text = "x".repeat(255);
    assert_eq!(Short::new(&text).expect("maximum short string").len(), 255);
    text.push('x');
    assert!(matches!(
        Short::new(&text),
        Err(failure::Error::Encoding(wire::Encode::Length {
            kind: wire::Encoding::Short,
            actual: 256,
            max: 255,
        }))
    ));
}
