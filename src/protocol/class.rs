#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Class(u16);

impl Class {
    pub(crate) const fn new(value: u16) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> u16 {
        self.0
    }
}

pub(crate) const CONNECTION: Class = Class::new(10);
pub(crate) const CHANNEL: Class = Class::new(20);
pub(crate) const EXCHANGE: Class = Class::new(40);
pub(crate) const QUEUE: Class = Class::new(50);
pub(crate) const BASIC: Class = Class::new(60);
pub(crate) const CONFIRM: Class = Class::new(85);
pub(crate) const TX: Class = Class::new(90);

const _: () = assert!(size_of::<Class>() == size_of::<u16>());
