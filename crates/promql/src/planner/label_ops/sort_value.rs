use super::{InstantSample, SampleValue};

/// Returns the float value of a sample, or `NaN` for a histogram sample.
///
/// This matches the interpreter's `float_sample_value(...).unwrap_or(f64::NAN)`
/// in the sort comparator.
pub(crate) fn sort_value(sample: &InstantSample) -> f64 {
    match sample.value {
        SampleValue::Float(value) => value,
        SampleValue::Histogram(_) => f64::NAN,
    }
}
