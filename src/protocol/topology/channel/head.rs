use crate::protocol::topology::records;

pub(in crate::protocol::topology) struct Head {
    pub(in crate::protocol::topology) tracked: bool,
    pub(in crate::protocol::topology) confirms: bool,
    pub(in crate::protocol::topology) qos: Option<records::Qos>,
}

impl Head {
    pub(super) const fn new() -> Self {
        Self {
            tracked: false,
            confirms: false,
            qos: None,
        }
    }

    pub(super) const fn is_tracked(&self) -> bool {
        self.tracked
    }

    pub(super) fn track(&mut self) {
        self.tracked = true;
    }

    pub(super) fn clear(&mut self) {
        self.tracked = false;
        self.confirms = false;
        self.qos = None;
    }
}
