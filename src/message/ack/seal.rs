use crate::message::ack;

pub trait Seal {}

impl Seal for ack::Auto {}
impl Seal for ack::Manual {}
