use super::{
    RangeSeries, Time, float_range_samples, note_histograms_ignored_in_range, predict_linear,
};

/// Folds one series' window into its `predict_linear` value.
///
/// The regression is taken over the window that `range_end_ms` closes, but the
/// prediction is anchored at `eval_ms`, the query's own evaluation time:
/// `funcPredictLinear` passes `enh.Ts` as the intercept time. An `@` modifier
/// therefore moves the window without moving what the prediction is relative
/// to, which is why `predict_linear(m[55m] @ 3000, 3600)` answers differently
/// at each evaluation time.
pub(crate) fn predict_linear_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    duration: Time,
    eval_ms: i64,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let samples = float_range_samples(series, range_end_ms, range);
    predict_linear(&samples, eval_ms, duration)
}
