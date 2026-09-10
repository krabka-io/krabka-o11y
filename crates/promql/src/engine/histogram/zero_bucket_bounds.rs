use super::{NativeHistogram, NativeQuantileBucket};

/// Narrows the zero bucket's bounds to the side the histogram actually observed.
///
/// Prometheus reads a zero bucket in a histogram that has only positive buckets
/// as starting at zero, and one in a histogram that has only negative buckets as
/// ending at zero, so that an interpolation inside it does not reach across a
/// side where nothing was observed. A histogram with buckets on both sides, or
/// on neither, keeps the threshold on both sides.
pub(crate) fn zero_bucket_bounds(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
) -> NativeQuantileBucket {
    let mut bucket = bucket;
    if hist.negative_counts.is_empty() && !hist.positive_counts.is_empty() {
        bucket.lower = 0.0;
    } else if hist.positive_counts.is_empty() && !hist.negative_counts.is_empty() {
        bucket.upper = 0.0;
    }
    bucket
}
