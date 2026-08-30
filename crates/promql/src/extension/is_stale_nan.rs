use super::*;

/// True when `value` is exactly Prometheus' stale-NaN marker and not some
/// genuine NaN. Genuine NaN values must stay as NaN samples.
#[must_use]
pub(crate) fn is_stale_nan(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}
