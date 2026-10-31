/// Server version, authentication mechanisms, and locales for Connection.Start.
#[derive(Clone, Copy)]
pub struct Greeting<'a> {
    major: u8,
    minor: u8,
    auth: &'a [u8],
    locales: &'a [u8],
}

impl<'a> Greeting<'a> {
    pub const PLAIN: Greeting<'static> = Greeting::new(0, 9, b"PLAIN", b"en_US");

    pub(crate) const fn new(major: u8, minor: u8, auth: &'a [u8], locales: &'a [u8]) -> Self {
        Self {
            major,
            minor,
            auth,
            locales,
        }
    }

    pub(crate) const fn parts(&self) -> (u8, u8, &'a [u8], &'a [u8]) {
        (self.major, self.minor, self.auth, self.locales)
    }
}
