use crate::message::content;
use crate::service::content::pending;

pub struct Returned<'c, 'b: 'c, 'd: 'b> {
    lease: pending::Lease<'c, 'b, 'd, content::ReturnText>,
}

impl<'c, 'b: 'c, 'd: 'b> Returned<'c, 'b, 'd> {
    pub(crate) const fn from_lease(lease: pending::Lease<'c, 'b, 'd, content::ReturnText>) -> Self {
        Self { lease }
    }

    pub fn reply_code(&self) -> u16 {
        self.lease.reply_code()
    }

    pub fn reply_text(&self) -> &str {
        self.lease.return_text(content::ReturnText::REPLY)
    }

    pub fn exchange(&self) -> &str {
        self.lease.return_text(content::ReturnText::EXCHANGE)
    }

    pub fn routing_key(&self) -> &str {
        self.lease.return_text(content::ReturnText::ROUTING_KEY)
    }

    pub fn properties(&self) -> content::Properties<'_, 'd> {
        self.lease.properties()
    }

    pub fn body(&self) -> content::Body<'_, 'b, 'd> {
        self.lease.body()
    }

    /// Returns the retained content slot to its pool.
    pub fn release(self) {
        self.lease.release();
    }
}

const _: () = assert!(size_of::<Returned<'static, 'static, 'static>>() == size_of::<usize>());
