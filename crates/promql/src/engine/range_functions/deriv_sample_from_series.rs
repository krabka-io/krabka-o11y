use super::{
    RangeSeries, Time, float_range_samples, note_histograms_ignored_in_range, regression_slope,
};

pub(crate) fn deriv_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let samples = float_range_samples(series, range_end_ms, range);
    regression_slope(&samples, range_end_ms)
}
