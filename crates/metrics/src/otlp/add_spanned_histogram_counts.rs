use super::{BucketSpan, spanned_histogram_counts, compact_spanned_histogram_counts};

pub(crate) fn add_spanned_histogram_counts(
    left_spans: &[BucketSpan],
    left_counts: &[f64],
    right_spans: &[BucketSpan],
    right_counts: &[f64],
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut buckets = spanned_histogram_counts(left_spans, left_counts);
    for (index, count) in spanned_histogram_counts(right_spans, right_counts) {
        *buckets.entry(index).or_insert(0.0) += count;
    }
    compact_spanned_histogram_counts(buckets)
}
