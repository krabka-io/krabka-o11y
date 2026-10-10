#[cfg(feature = "experimental-functions")]
use super::double_exponential_smoothing_sample_from_series;
use super::{
    Labels, OuterRangeFn, RangeSeries, RangeWindow, SampleValue, deriv_sample_from_series,
    instant_delta_sample_from_series, over_time_sample_from_series,
    predict_linear_sample_from_series, quantile_over_time_sample_from_series,
    range_function_sample_from_series,
};

/// The range function an outer fold applies, and when the query is evaluated.
#[derive(Clone, Copy)]
pub(crate) struct OuterRangeFold {
    /// The function folded over each series' window.
    pub(crate) outer: OuterRangeFn,
    /// Where the query is evaluated, in milliseconds.
    pub(crate) eval_ms: i64,
}

/// Folds one series' window into its `(result labels, value)`.
///
/// The fold matches what each interpreter `eval_*_call` does per series. This
/// function returns `None` for a no-value window, and the result drops that
/// series.
///
/// `range_end_ms` is where the WINDOW ends, which an `@` modifier or an offset
/// moves; `eval_ms` is where the QUERY is evaluated. Only `predict_linear`
/// needs to tell them apart, and it predicts from the evaluation time.
pub(crate) fn outer_range_sample_from_series(
    series: &RangeSeries,
    window: &RangeWindow,
    fold: OuterRangeFold,
) -> Option<(Labels, SampleValue)> {
    let RangeWindow {
        end_ms: range_end_ms,
        range,
        ..
    } = *window;
    let OuterRangeFold { outer, eval_ms } = fold;
    let drop_range_name = |labels: &Labels| labels.clone();
    match outer {
        OuterRangeFn::Range(kind) => range_function_sample_from_series(series, window, kind)
            .map(|value| (drop_range_name(&series.labels), value)),
        OuterRangeFn::InstantDelta(kind) => {
            instant_delta_sample_from_series(series, range_end_ms, range, kind)
                .map(|value| (drop_range_name(&series.labels), value))
        }
        OuterRangeFn::Deriv => deriv_sample_from_series(series, range_end_ms, range)
            .map(|value| (series.labels.clone(), SampleValue::Float(value))),
        OuterRangeFn::OverTime(kind) => {
            over_time_sample_from_series(series, range_end_ms, range, kind).map(|value| {
                let labels = series.labels.clone();
                (labels, value)
            })
        }
        OuterRangeFn::QuantileOverTime(quantile) => {
            quantile_over_time_sample_from_series(series, range_end_ms, range, quantile)
                .map(|value| (series.labels.clone(), SampleValue::Float(value)))
        }
        OuterRangeFn::PredictLinear(duration) => {
            predict_linear_sample_from_series(series, range_end_ms, range, duration, eval_ms)
                .map(|value| (series.labels.clone(), SampleValue::Float(value)))
        }
        #[cfg(feature = "experimental-functions")]
        OuterRangeFn::DoubleExponentialSmoothing { smoothing, trend } => {
            double_exponential_smoothing_sample_from_series(
                series,
                range_end_ms,
                range,
                smoothing,
                trend,
            )
            .map(|value| (series.labels.clone(), SampleValue::Float(value)))
        }
    }
}
