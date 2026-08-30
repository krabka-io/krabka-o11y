use super::*;

pub(crate) fn nh(
    count: f64,
    sum: f64,
    schema: i8,
    positive_buckets: &[(i32, f64)],
) -> NativeHistogram {
    let mut buckets = positive_buckets.to_vec();
    buckets.sort_by_key(|(index, _)| *index);
    let (positive_spans, positive_counts) = spans_and_counts(&buckets);
    NativeHistogram {
        schema,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum,
        positive_spans,
        positive_counts,
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    }
}
