use super::*;

pub(crate) fn predict_linear_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    duration: Time,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range);
    predict_linear(&samples, range_end_ms, duration)
}
