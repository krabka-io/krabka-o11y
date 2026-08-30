use super::{BucketSpan, spanned_histogram_counts, compact_spanned_histogram_counts};

pub(crate) fn add_histogram_counts(
    start_spans: &[BucketSpan],
    start_counts: &[f64],
    step_spans: &[BucketSpan],
    step_counts: &[f64],
    multiplier: f64,
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut buckets = spanned_histogram_counts(start_spans, start_counts);
    for (index, count) in spanned_histogram_counts(step_spans, step_counts) {
        *buckets.entry(index).or_insert(0.0) += count * multiplier;
    }
    compact_spanned_histogram_counts(buckets)
}
