use std::mem;

use dope_amqp091::service::Port;
use dope_amqp091_fiber::conn::Connection;

#[test]
fn keeps_handle_alignment() {
    assert_eq!(
        mem::align_of::<Connection<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>(),
        mem::align_of::<&Port<'static, 'static, 'static, 'static>>()
    );
}
