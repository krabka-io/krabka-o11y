use super::*;

pub(crate) fn native_histogram() -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: false,
        reset_hint: ResetHint::No,
        zero_threshold: 1e-128,
        zero_count: 0.0,
        count: 2.0,
        sum: 3.0,
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 1,
        }],
        positive_counts: vec![2.0],
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}
