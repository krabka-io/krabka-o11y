use super::*;

pub(crate) fn native_histogram_bucket_quantile(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
    fraction: f64,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower.abs() * (bucket.upper.abs() / bucket.lower.abs()).powf(fraction));
    }
    bucket.lower * (bucket.upper / bucket.lower).powf(fraction)
}
