use super::{BucketSpan, compact_spanned_histogram_counts, spanned_histogram_counts};
use crate::SpannedHistogramCounts;

pub(crate) fn add_spanned_histogram_counts(
    left_spans: &[BucketSpan],
    left_counts: &[f64],
    right_spans: &[BucketSpan],
    right_counts: &[f64],
) -> SpannedHistogramCounts {
    let mut buckets = spanned_histogram_counts(left_spans, left_counts);
    for bucket in spanned_histogram_counts(right_spans, right_counts) {
        *buckets.entry(bucket.0).or_insert(0.0) += bucket.1;
    }
    compact_spanned_histogram_counts(buckets)
}
