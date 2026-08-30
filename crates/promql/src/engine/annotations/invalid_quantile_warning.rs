/// Exact Prometheus `InvalidQuantileWarning` text for a bad phi.
///
/// A bad phi is a `quantile` or `quantile_over_time` phi outside `[0, 1]`, or
/// NaN. Prometheus does not abort on a bad phi. It returns signed `+/-Inf` or
/// `NaN` and raises this warning, the same as the `histogram_quantile` family.
/// `got` renders through the canonical Prometheus float formatter, which
/// matches Go's `%v`.
pub(crate) fn invalid_quantile_warning(got: f64) -> String {
    format!(
        "PromQL warning: quantile value should be between 0 and 1, got {}",
        crate::http_api::format_sample_value(got)
    )
}
