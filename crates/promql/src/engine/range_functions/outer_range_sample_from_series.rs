use super::{
    ExtendedSelectorModifier, Labels, OuterRangeFn, RangeSeries, SampleValue, Time,
    deriv_sample_from_series, instant_delta_sample_from_series, labels_without_metric_name,
    over_time_sample_from_series, predict_linear_sample_from_series,
    quantile_over_time_sample_from_series, range_function_sample_from_series,
};

/// Folds one series' window into its `(result labels, value)`.
///
/// The fold matches what each interpreter `eval_*_call` does per series. This
/// function returns `None` for a no-value window, and the result drops that
/// series.
pub(crate) fn outer_range_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    outer: OuterRangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<(Labels, SampleValue)> {
    match outer {
        OuterRangeFn::Range(kind) => {
            range_function_sample_from_series(series, range_end_ms, range, kind, modifier)
                .map(|value| (labels_without_metric_name(&series.labels), value))
        }
        OuterRangeFn::InstantDelta(kind) => {
            instant_delta_sample_from_series(series, range_end_ms, range, kind).map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
        OuterRangeFn::Deriv => deriv_sample_from_series(series, range_end_ms, range).map(|value| {
            (
                labels_without_metric_name(&series.labels),
                SampleValue::Float(value),
            )
        }),
        OuterRangeFn::OverTime(kind) => {
            over_time_sample_from_series(series, range_end_ms, range, kind).map(|value| {
                let labels = if kind.preserves_metric_name() {
                    series.labels.clone()
                } else {
                    labels_without_metric_name(&series.labels)
                };
                (labels, value)
            })
        }
        OuterRangeFn::QuantileOverTime(quantile) => {
            quantile_over_time_sample_from_series(series, range_end_ms, range, quantile).map(
                |value| {
                    (
                        labels_without_metric_name(&series.labels),
                        SampleValue::Float(value),
                    )
                },
            )
        }
        OuterRangeFn::PredictLinear(duration) => {
            predict_linear_sample_from_series(series, range_end_ms, range, duration).map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
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
            .map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
    }
}
