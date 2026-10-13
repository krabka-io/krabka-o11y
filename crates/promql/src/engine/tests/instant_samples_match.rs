use super::*;

/// Compares two sorted instant-sample vectors bit-exactly for the parity tests.
///
/// A bit-exact float comparison makes a genuine NaN equal a genuine NaN.
/// `PartialEq` on `SampleValue::Float` uses IEEE `==`, and `NaN != NaN` under
/// that rule, so a plain `assert_eq!` fails when a path correctly keeps a
/// genuine NaN value. The tests do not expect stale-NaN markers to survive
/// selection on either path, so the markers never reach this comparison.
pub(crate) fn instant_samples_match(
    left: &[crate::InstantSample],
    right: &[crate::InstantSample],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for index in 0..left.len() {
        let left_sample = &left[index];
        let right_sample = &right[index];
        if left_sample.labels != right_sample.labels
            || left_sample.ts_ms != right_sample.ts_ms
            || !sample_values_match(&left_sample.value, &right_sample.value)
        {
            return false;
        }
    }
    true
}

fn sample_values_match(left: &SampleValue, right: &SampleValue) -> bool {
    match left {
        SampleValue::Float(left_value) => match right {
            SampleValue::Float(right_value) => left_value.to_bits() == right_value.to_bits(),
            SampleValue::Histogram(_) => false,
        },
        SampleValue::Histogram(left_histogram) => match right {
            SampleValue::Float(_) => false,
            SampleValue::Histogram(right_histogram) => left_histogram == right_histogram,
        },
    }
}
