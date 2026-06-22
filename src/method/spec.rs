pub struct ClassId;

impl ClassId {
    pub const CONNECTION: u16 = 10;
    pub const CHANNEL: u16 = 20;
    pub const EXCHANGE: u16 = 40;
    pub const QUEUE: u16 = 50;
    pub const BASIC: u16 = 60;
    pub const CONFIRM: u16 = 85;
    pub const TX: u16 = 90;
}

pub struct Connection;

impl Connection {
    pub const START: u16 = 10;
    pub const START_OK: u16 = 11;
    pub const SECURE: u16 = 20;
    pub const SECURE_OK: u16 = 21;
    pub const TUNE: u16 = 30;
    pub const TUNE_OK: u16 = 31;
    pub const OPEN: u16 = 40;
    pub const OPEN_OK: u16 = 41;
    pub const CLOSE: u16 = 50;
    pub const CLOSE_OK: u16 = 51;
    pub const BLOCKED: u16 = 60;
    pub const UNBLOCKED: u16 = 61;
}

pub struct Channel;

impl Channel {
    pub const OPEN: u16 = 10;
    pub const OPEN_OK: u16 = 11;
    pub const FLOW: u16 = 20;
    pub const FLOW_OK: u16 = 21;
    pub const CLOSE: u16 = 40;
    pub const CLOSE_OK: u16 = 41;
}

pub struct Exchange;

impl Exchange {
    pub const DECLARE: u16 = 10;
    pub const DECLARE_OK: u16 = 11;
    pub const DELETE: u16 = 20;
    pub const DELETE_OK: u16 = 21;
    pub const BIND: u16 = 30;
    pub const BIND_OK: u16 = 31;
    pub const UNBIND: u16 = 40;
    pub const UNBIND_OK: u16 = 51;
}

pub struct Queue;

impl Queue {
    pub const DECLARE: u16 = 10;
    pub const DECLARE_OK: u16 = 11;
    pub const BIND: u16 = 20;
    pub const BIND_OK: u16 = 21;
    pub const PURGE: u16 = 30;
    pub const PURGE_OK: u16 = 31;
    pub const DELETE: u16 = 40;
    pub const DELETE_OK: u16 = 41;
    pub const UNBIND: u16 = 50;
    pub const UNBIND_OK: u16 = 51;
}

pub struct Basic;

impl Basic {
    pub const QOS: u16 = 10;
    pub const QOS_OK: u16 = 11;
    pub const CONSUME: u16 = 20;
    pub const CONSUME_OK: u16 = 21;
    pub const CANCEL: u16 = 30;
    pub const CANCEL_OK: u16 = 31;
    pub const PUBLISH: u16 = 40;
    pub const RETURN: u16 = 50;
    pub const DELIVER: u16 = 60;
    pub const GET: u16 = 70;
    pub const GET_OK: u16 = 71;
    pub const GET_EMPTY: u16 = 72;
    pub const ACK: u16 = 80;
    pub const REJECT: u16 = 90;
    pub const RECOVER_ASYNC: u16 = 100;
    pub const RECOVER: u16 = 110;
    pub const RECOVER_OK: u16 = 111;
    pub const NACK: u16 = 120;
}

pub struct Confirm;

impl Confirm {
    pub const SELECT: u16 = 10;
    pub const SELECT_OK: u16 = 11;
}

pub struct Tx;

impl Tx {
    pub const SELECT: u16 = 10;
    pub const SELECT_OK: u16 = 11;
    pub const COMMIT: u16 = 20;
    pub const COMMIT_OK: u16 = 21;
    pub const ROLLBACK: u16 = 30;
    pub const ROLLBACK_OK: u16 = 31;
}

pub trait Method {
    const CLASS_ID: u16;
    const METHOD_ID: u16;
}
