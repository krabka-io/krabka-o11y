use super::{NativeHistogram, NativeQuantileBucket, native_histogram_buckets};

/// Every populated bucket of `hist`, in ascending order of value.
///
/// This is `FloatHistogram.AllBucketIterator`: the negative buckets from the
/// most negative upwards, then the zero bucket where it holds anything, then
/// the positive buckets. Both `histogram_quantile` and `histogram_fraction`
/// walk the buckets in that order and accumulate a rank as they go, so the
/// order is part of the answer.
pub(crate) fn native_histogram_all_buckets(hist: &NativeHistogram) -> Vec<NativeQuantileBucket> {
    let mut buckets = native_histogram_buckets(hist);
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    buckets
}
