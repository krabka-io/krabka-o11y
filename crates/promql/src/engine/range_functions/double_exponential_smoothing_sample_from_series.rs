#[cfg(feature = "experimental-functions")]
use super::{
    RangeSeries, Time, double_exponential_smoothing, float_range_samples,
    note_histograms_ignored_in_range,
};

#[cfg(feature = "experimental-functions")]
pub(crate) fn double_exponential_smoothing_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let samples = float_range_samples(series, range_end_ms, range);
    double_exponential_smoothing(&samples, smoothing_factor, trend_factor)
}
