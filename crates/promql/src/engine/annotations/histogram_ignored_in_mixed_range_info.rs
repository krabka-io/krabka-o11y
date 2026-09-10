/// Exact Prometheus `HistogramIgnoredInMixedRangeInfo` text for `metric`.
pub(crate) fn histogram_ignored_in_mixed_range_info(metric: &str) -> String {
    format!(
        "PromQL info: ignored histograms in a range containing both floats and histograms for metric name {metric:?}"
    )
}
