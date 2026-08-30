use super::*;

pub(crate) fn native_histogram_quantile(quantile: f64, hist: &NativeHistogram) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mut buckets = native_histogram_buckets(hist);
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    let rank = quantile * hist.count;
    let mut cumulative = 0.0;
    for bucket in buckets {
        let previous = cumulative;
        cumulative += bucket.count;
        if cumulative < rank {
            continue;
        }
        if bucket.count <= 0.0 {
            return bucket.upper;
        }
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            return bucket.upper;
        }
        if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return bucket.lower;
        }
        return native_histogram_bucket_quantile(hist, bucket, (rank - previous) / bucket.count);
    }
    f64::NAN
}
