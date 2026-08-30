use super::{RangeSeries, Time, float_range_samples, regression_slope};

pub(crate) fn deriv_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range);
    regression_slope(&samples, range_end_ms)
}
