use super::*;

pub(crate) fn native_histogram_with_positive_buckets(
    count: f64,
    sum: f64,
    positive_spans: Vec<BucketSpan>,
    positive_counts: Vec<f64>,
) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum,
        positive_spans,
        positive_counts,
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}
