use super::{BucketCount, SpansAndCounts, *};

pub(crate) fn nh(
    count: f64,
    sum: f64,
    schema: i8,
    positive_buckets: &[BucketCount],
) -> NativeHistogram {
    let mut buckets = positive_buckets.to_vec();
    buckets.sort_by_key(|bucket| bucket.index);
    let SpansAndCounts {
        spans: positive_spans,
        counts: positive_counts,
    } = spans_and_counts(&buckets);
    NativeHistogram::builder()
        .schema(schema)
        .is_float(true)
        .reset_hint(ResetHint::No)
        .zero_threshold(0.0)
        .zero_count(0.0)
        .count(count)
        .sum(sum)
        .positive_spans(positive_spans)
        .positive_counts(positive_counts)
        .negative_spans(vec![])
        .negative_counts(vec![])
        .maybe_custom_values(None)
        .maybe_start_timestamp_ms(None)
        .build()
}

pub(crate) fn two_sided_histogram(
    count: f64,
    sum: f64,
    reset_hint: ResetHint,
    start_timestamp_ms: Option<i64>,
) -> NativeHistogram {
    NativeHistogram::builder()
        .schema(0)
        .is_float(true)
        .reset_hint(reset_hint)
        .zero_threshold(0.001)
        .zero_count(2.0)
        .count(count)
        .sum(sum)
        .positive_spans(vec![BucketSpan {
            offset: 0,
            length: 2,
        }])
        .positive_counts(vec![1.0, 2.0])
        .negative_spans(vec![BucketSpan {
            offset: 0,
            length: 2,
        }])
        .negative_counts(vec![1.0, 2.0])
        .maybe_custom_values(None)
        .maybe_start_timestamp_ms(start_timestamp_ms)
        .build()
}
