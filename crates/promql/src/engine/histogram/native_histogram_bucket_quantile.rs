use super::{NativeHistogram, NativeQuantileBucket};

/// Interpolates `fraction` of the way through one bucket.
///
/// Custom buckets and the zero bucket interpolate LINEARLY, matching the
/// classic-histogram convention. An exponential bucket interpolates on a
/// logarithmic scale, where every bucket of a schema has the same width, so the
/// same fraction applies to the logarithms of the bounds and the result comes
/// back through `exp2`. A negative bucket mirrors that.
pub(crate) fn native_histogram_bucket_quantile(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
    fraction: f64,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    if bucket.lower > 0.0 {
        return (log_lower + (log_upper - log_lower) * fraction).exp2();
    }
    -(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2()
}
