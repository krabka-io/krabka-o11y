use super::*;

pub(crate) fn native_histogram(count: f64, sum: f64) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum,
        positive_spans: vec![],
        positive_counts: vec![],
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    }
}
