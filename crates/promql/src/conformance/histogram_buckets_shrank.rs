use super::{BucketSpan, spanned_histogram_counts};

/// Whether any populated bucket of `last` lost population in `next`.
///
/// Prometheus's chunk appender treats a bucket that disappears, or whose
/// count falls, as a counter reset. A bucket that was already empty may
/// vanish without meaning anything.
pub(crate) fn histogram_buckets_shrank(
    last_spans: &[BucketSpan],
    last_counts: &[f64],
    next_spans: &[BucketSpan],
    next_counts: &[f64],
) -> bool {
    let next = spanned_histogram_counts(next_spans, next_counts);
    spanned_histogram_counts(last_spans, last_counts)
        .into_iter()
        .filter(|entry| entry.1 > 0.0)
        .any(|entry| next.get(&entry.0).copied().unwrap_or(0.0) < entry.1)
}
