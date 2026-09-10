/// Exact Prometheus `HistogramCounterResetCollisionWarning` text.
///
/// `operation` is Prometheus' own `HistogramOperation` word: `addition`,
/// `subtraction`, or `aggregation`.
pub(crate) fn histogram_counter_reset_collision_warning(operation: &str) -> String {
    format!("PromQL warning: conflicting counter resets during histogram {operation}")
}
