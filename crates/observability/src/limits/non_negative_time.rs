use serde::{Deserializer, de::Error as _};

use super::{Time, TimeExt, serde_units};

/// Reads a time cap and rejects a negative one.
///
/// `human::time` accepts a signed magnitude, and every check in this crate
/// applies only a cap greater than zero. A cap of `"-1s"` would therefore load
/// cleanly and mean *unlimited*, but zero is the documented way to turn a cap
/// off. A rejection at parse time keeps one sentinel.
///
/// There is no `serialize` here. The write side needs no guard, because a
/// [`Limits`](super::Limits) in memory has already been through this one, and
/// `serde_units` writes the human form directly.
///
/// # Errors
///
/// If the value is not a human time string, or names a negative extent.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Time, D::Error> {
    let value = serde_units::human::time::deserialize(deserializer)?;
    if value < Time::ZERO {
        return Err(D::Error::custom(
            "a time limit cannot be negative; use 0 to turn the limit off",
        ));
    }
    Ok(value)
}
