use super::{
    NativeHistogram, NativeQuantileBucket, append_native_spanned_buckets, custom_histogram_bound,
    native_histogram_bucket_mean, native_histogram_buckets,
};

/// The population variance of `hist`, as `histogram_stdvar` gives it.
///
/// This follows `histogramVariance` in Prometheus `promql/functions.go`. It
/// skips the empty buckets. A custom bucket has the bounds of `getBound`, so
/// the first custom bucket starts at `-Inf` and the last ends at `+Inf`. An
/// open bucket that holds an observation makes the variance infinite.
pub(crate) fn native_histogram_stdvar(hist: &NativeHistogram) -> f64 {
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mean = hist.sum / hist.count;
    variance_buckets(hist)
        .into_iter()
        .filter(|bucket| bucket.count != 0.0)
        .map(|bucket| {
            let bucket_mean = native_histogram_bucket_mean(hist, bucket);
            bucket.count * (bucket_mean - mean).powi(2)
        })
        .sum::<f64>()
        / hist.count
}

fn variance_buckets(hist: &NativeHistogram) -> Vec<NativeQuantileBucket> {
    if !hist.is_nhcb() {
        return native_histogram_buckets(hist);
    }
    let custom_values = hist.custom_values.as_deref().unwrap_or_default();
    let mut buckets = Vec::new();
    append_native_spanned_buckets(
        &mut buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| NativeQuantileBucket {
            // `native_histogram_buckets` starts the first bucket at zero when
            // its upper bound is positive, as `histogram_quantile` does.
            // `getBound` starts it at `-Inf`.
            lower: if index == 0 {
                f64::NEG_INFINITY
            } else {
                custom_histogram_bound(index - 1, custom_values)
            },
            upper: custom_histogram_bound(index, custom_values),
            count: 0.0,
        },
    );
    buckets
}
