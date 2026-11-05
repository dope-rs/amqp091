use std::ptr;

use super::Auth;

impl Drop for Auth {
    fn drop(&mut self) {
        for byte in self.response.as_mut_slice() {
            // SAFETY: Auth exclusively owns this live, initialized byte until deallocation.
            unsafe { ptr::write_volatile(byte, 0) };
        }
    }
}
