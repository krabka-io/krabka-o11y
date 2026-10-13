use super::*;

/// Compares sorted instant samples for parity checks.
///
/// When `nan_equal` is false, float values must be bit-exact. When true, any
/// two NaN values compare equal, matching Prometheus semantics.
pub(crate) fn instant_samples_match(
    left: &[crate::InstantSample],
    right: &[crate::InstantSample],
) -> bool {
    samples_match(left, right, false)
}

pub(crate) fn nan_equal_samples(
    left: &[crate::InstantSample],
    right: &[crate::InstantSample],
) -> bool {
    samples_match(left, right, true)
}

fn samples_match(
    left: &[crate::InstantSample],
    right: &[crate::InstantSample],
    nan_equal: bool,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for (left_sample, right_sample) in left.iter().zip(right) {
        if left_sample.labels != right_sample.labels || left_sample.ts_ms != right_sample.ts_ms {
            return false;
        }
        match (&left_sample.value, &right_sample.value) {
            (SampleValue::Float(left), SampleValue::Float(right)) => {
                if left.to_bits() != right.to_bits()
                    && !(nan_equal && left.is_nan() && right.is_nan())
                {
                    return false;
                }
            }
            (SampleValue::Histogram(left), SampleValue::Histogram(right)) if left == right => {}
            _ => return false,
        }
    }
    true
}
