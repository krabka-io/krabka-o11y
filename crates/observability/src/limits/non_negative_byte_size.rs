use serde::{Deserializer, de::Error as _};

use super::{ByteSize, ByteSizeExt, serde_units};

/// Reads a size cap and rejects a negative one.
///
/// The reasoning matches [`non_negative_time`](super::non_negative_time): zero
/// is the one sentinel for "no cap", so a negative size is refused rather than
/// read as a second one.
///
/// # Errors
///
/// If the value is not a human size string, or names a negative size.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
    let value = serde_units::human::byte_size::deserialize(deserializer)?;
    if value < ByteSize::ZERO {
        return Err(D::Error::custom(
            "a size limit cannot be negative; use 0 to turn the limit off",
        ));
    }
    Ok(value)
}
