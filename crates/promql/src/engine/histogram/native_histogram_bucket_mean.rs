use super::{NativeHistogram, NativeQuantileBucket};

pub(crate) fn native_histogram_bucket_mean(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
) -> f64 {
    if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
        return bucket.upper;
    }
    if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
        return bucket.lower;
    }
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return f64::midpoint(bucket.lower, bucket.upper);
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower * bucket.upper).sqrt();
    }
    (bucket.lower * bucket.upper).sqrt()
}
