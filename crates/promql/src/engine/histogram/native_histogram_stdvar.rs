use super::*;

pub(crate) fn native_histogram_stdvar(hist: &NativeHistogram) -> f64 {
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mean = hist.sum / hist.count;
    native_histogram_buckets(hist)
        .into_iter()
        .map(|bucket| {
            let bucket_mean = native_histogram_bucket_mean(hist, bucket);
            bucket.count * (bucket_mean - mean).powi(2)
        })
        .sum::<f64>()
        / hist.count
}
