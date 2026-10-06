use super::maybe_add_metric_name;

/// Exact Prometheus `NativeHistogramFractionNaNsInfo` text for `metric`.
pub(crate) fn native_histogram_fraction_nans_info(metric: &str) -> String {
    maybe_add_metric_name("PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions".to_string(), metric)
}
