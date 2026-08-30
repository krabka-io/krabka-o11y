
#[cfg(feature = "experimental-functions")]
pub(crate) fn double_exponential_smoothing_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range);
    double_exponential_smoothing(&samples, smoothing_factor, trend_factor)
}
