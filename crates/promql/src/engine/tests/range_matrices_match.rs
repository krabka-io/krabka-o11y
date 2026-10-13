use super::*;

/// Compares two `RangeMatrix` results for the range parity test.
///
/// The comparison is bit-exact on float sample values, so a genuine NaN equals
/// a genuine NaN. A plain `PartialEq` fails on `NaN == NaN`. Series order,
/// labelsets, per-step timestamps, and gaps must all match.
pub(crate) fn range_matrices_match(left: &QueryResult, right: &QueryResult) -> bool {
    let QueryResult::RangeMatrix(left) = left else {
        return false;
    };
    let QueryResult::RangeMatrix(right) = right else {
        return false;
    };
    if left.len() != right.len() {
        return false;
    }
    let mut series_index = 0;
    while series_index < left.len() {
        let left_series = &left[series_index];
        let right_series = &right[series_index];
        if left_series.labels != right_series.labels
            || left_series.samples.len() != right_series.samples.len()
        {
            return false;
        }
        let mut sample_index = 0;
        while sample_index < left_series.samples.len() {
            let left_sample = &left_series.samples[sample_index];
            let right_sample = &right_series.samples[sample_index];
            if left_sample.timestamp_ms != right_sample.timestamp_ms {
                return false;
            }
            let values_match = if let SampleValue::Float(left_value) = &left_sample.value {
                if let SampleValue::Float(right_value) = &right_sample.value {
                    left_value.to_bits() == right_value.to_bits()
                } else {
                    false
                }
            } else {
                left_sample.value == right_sample.value
            };
            if !values_match {
                return false;
            }
            sample_index += 1;
        }
        series_index += 1;
    }
    true
}
