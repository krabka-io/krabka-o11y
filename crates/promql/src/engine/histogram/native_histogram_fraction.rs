use super::*;

pub(crate) fn native_histogram_fraction(lower: f64, upper: f64, hist: &NativeHistogram) -> f64 {
    if lower.is_nan() || upper.is_nan() || hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let in_range = native_histogram_buckets(hist)
        .into_iter()
        .map(|bucket| bucket.count * bucket_overlap_fraction(bucket, lower, upper))
        .sum::<f64>();
    in_range / hist.count
}
