use super::{
    ClassicBucket, HistogramReducers, InstantSample, NativeHistogram, Result,
    apply_histogram_reduction, classic_histogram_quantile,
    emit_histogram_quantile_forced_monotonicity_info, emit_warning, invalid_quantile_warning,
    is_valid_quantile, native_histogram_quantile,
};

/// Prometheus. Both the `__name__` and `le` labels are dropped from every output
/// series. Classic output samples carry `time_ms`; native ones keep the source
/// sample timestamp.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(crate) fn apply_histogram_quantile(
    quantile: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    // `funcHistogramQuantile` warns about an out-of-range quantile before it
    // looks at a single sample, so an empty input vector still warns.
    if !is_valid_quantile(quantile) {
        emit_warning(invalid_quantile_warning(quantile));
    }
    apply_histogram_reduction(
        samples,
        time_ms,
        HistogramReducers {
            native: |histogram: &NativeHistogram, metric: &str| {
                native_histogram_quantile(quantile, histogram, metric)
            },
            classic: |metric: &str, buckets: &mut Vec<ClassicBucket>| {
                let (quantile_value, forced, repairs) =
                    classic_histogram_quantile(quantile, buckets);
                if forced {
                    emit_histogram_quantile_forced_monotonicity_info(metric, time_ms, repairs);
                }
                quantile_value
            },
        },
    )
}
