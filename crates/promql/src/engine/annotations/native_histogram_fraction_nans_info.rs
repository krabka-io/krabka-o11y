/// Exact Prometheus `NativeHistogramFractionNaNsInfo` text for `metric`.
pub(crate) fn native_histogram_fraction_nans_info(_metric: &str) -> String {
    "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions".to_string()
}
