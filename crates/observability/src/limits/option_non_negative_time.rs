use serde::{Deserializer, de::Error as _};

use super::{Time, TimeExt, serde_units};

/// Reads the optional time cap and rejects a negative one.
///
/// The override path deserializes through `PartialLimits` and not `Limits`, so
/// the guard exists on both. Without it a per-tenant override slips past the
/// check its `Limits` twin makes. This module is deserialize-only, because
/// nothing serializes `PartialLimits`.
///
/// # Errors
///
/// If the value is not a human time string, or names a negative extent.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Time>, D::Error> {
    let value = serde_units::human::option_time::deserialize(deserializer)?;
    if value.is_some_and(|value| value < Time::ZERO) {
        return Err(D::Error::custom(
            "a time limit cannot be negative; use 0 to turn the limit off",
        ));
    }
    Ok(value)
}
