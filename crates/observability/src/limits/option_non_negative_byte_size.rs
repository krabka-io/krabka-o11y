use serde::{Deserializer, de::Error as _};

use super::{ByteSize, ByteSizeExt, serde_units};

/// Reads the optional size cap and rejects a negative one.
///
/// # Errors
///
/// If the value is not a human size string, or names a negative size.
pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ByteSize>, D::Error> {
    let value = serde_units::human::option_byte_size::deserialize(deserializer)?;
    if value.is_some_and(|value| value < ByteSize::ZERO) {
        return Err(D::Error::custom(
            "a size limit cannot be negative; use 0 to turn the limit off",
        ));
    }
    Ok(value)
}
