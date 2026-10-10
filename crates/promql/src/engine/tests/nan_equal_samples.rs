use super::*;

/// NaN-aware sample comparison for the parity tests: labels and timestamps
/// must match exactly, and float values match when bit-equal or both NaN
/// (Prometheus treats all NaNs alike). A histogram value never matches.
pub(crate) fn nan_equal_samples(
    left: &[crate::InstantSample],
    right: &[crate::InstantSample],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).all(|(a, b)| {
        a.labels == b.labels
            && a.ts_ms == b.ts_ms
            && match (&a.value, &b.value) {
                (SampleValue::Float(x), SampleValue::Float(y)) => {
                    x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                }
                _ => false,
            }
    })
}
