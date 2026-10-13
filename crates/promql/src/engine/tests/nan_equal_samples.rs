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
    for index in 0..left.len() {
        let left_sample = &left[index];
        let right_sample = &right[index];
        if left_sample.labels != right_sample.labels || left_sample.ts_ms != right_sample.ts_ms {
            return false;
        }
        match &left_sample.value {
            SampleValue::Float(left_value) => match &right_sample.value {
                SampleValue::Float(right_value) => {
                    if left_value.to_bits() != right_value.to_bits()
                        && !(left_value.is_nan() && right_value.is_nan())
                    {
                        return false;
                    }
                }
                SampleValue::Histogram(_) => return false,
            },
            SampleValue::Histogram(_) => return false,
        }
    }
    true
}
