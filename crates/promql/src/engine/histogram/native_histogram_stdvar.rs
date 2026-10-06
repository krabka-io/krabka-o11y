use super::{
    NativeHistogram, NativeQuantileBucket, append_native_spanned_buckets, custom_histogram_bound,
    native_histogram_bucket_mean, native_histogram_buckets,
};
use crate::engine::range_functions::kahan_sum_inc;

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
    let mut variance = 0.0;
    let mut compensation = 0.0;
    for bucket in variance_buckets(hist) {
        if bucket.count == 0.0 {
            continue;
        }
        let delta = native_histogram_bucket_mean(hist, bucket) - mean;
        (variance, compensation) =
            kahan_sum_inc(bucket.count * delta * delta, variance, compensation);
    }
    (variance + compensation) / hist.count
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

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metrics::{BucketSpan, ResetHint};

    use super::*;

    #[test]
    fn histogram_variance_retains_small_bucket_contributions() {
        let histogram = NativeHistogram {
            schema: -53,
            is_float: true,
            reset_hint: ResetHint::Unknown,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 3.0,
            sum: 0.0,
            positive_spans: vec![BucketSpan {
                offset: 1,
                length: 3,
            }],
            positive_counts: vec![1.0, 1.0, 1.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: Some(vec![-200_000_000.0, 0.0, 2.0, 4.0]),
            start_timestamp_ms: None,
        };
        // The provided sum gives mean zero. Bucket midpoints -1e8, 1, and 3
        // contribute exactly 1e16 + 1 + 9, before division by three. A plain
        // left-to-right sum loses the unit contribution at the first addition.
        let expected = 10_000_000_000_000_010.0_f64 / 3.0;
        assert!(native_histogram_stdvar(&histogram).to_bits() == expected.to_bits());
    }
}
