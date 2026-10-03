use super::{NativeHistogram, NativeQuantileBucket};

/// The value that `histogram_stdvar` gives to the observations of `bucket`.
///
/// A custom bucket and the zero bucket take the arithmetic midpoint, so an
/// open custom bucket takes an infinite value. An exponential bucket takes the
/// geometric midpoint, negated below zero.
pub(crate) fn native_histogram_bucket_mean(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return f64::midpoint(bucket.lower, bucket.upper);
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower * bucket.upper).sqrt();
    }
    (bucket.lower * bucket.upper).sqrt()
}
