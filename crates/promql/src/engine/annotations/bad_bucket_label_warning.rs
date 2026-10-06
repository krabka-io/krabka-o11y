use super::maybe_add_metric_name;

/// Exact Prometheus `BadBucketLabelWarning` text for `metric`.
///
/// `label` is the `le` value that could not be read, which is the empty string
/// where the series carries no `le` at all.
pub(crate) fn bad_bucket_label_warning(label: &str, metric: &str) -> String {
    maybe_add_metric_name(
        format!(
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of {label:?}"
        ),
        metric,
    )
}
