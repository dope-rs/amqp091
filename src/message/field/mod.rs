pub use table::Table;
pub use value::Value;

use crate::failure;
use crate::failure::wire::{Encode, Encoding};

mod table;
mod value;
pub mod view;

fn sum(kind: Encoding, left: u32, right: u32) -> Result<u32, failure::Error> {
    left.checked_add(right)
        .ok_or(failure::Error::Encoding(Encode::Sum {
            kind,
            left,
            right,
            max: u32::MAX,
        }))
}
