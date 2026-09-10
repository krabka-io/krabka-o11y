use super::maybe_with_metric_name;

/// Exact Prometheus `NativeHistogramFractionNaNsInfo` text for `metric`.
pub(crate) fn native_histogram_fraction_nans_info(metric: &str) -> String {
    maybe_with_metric_name(
        "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions",
        metric,
    )
}
