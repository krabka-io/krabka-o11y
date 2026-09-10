/// Exact Prometheus `MismatchedCustomBucketsHistogramsInfo` text for `operation`.
///
/// `operation` is Prometheus' own `HistogramOperation` word: `addition`,
/// `subtraction`, or `aggregation`.
pub(crate) fn mismatched_custom_buckets_info(operation: &str) -> String {
    format!("PromQL info: mismatched custom buckets were reconciled during {operation}")
}
