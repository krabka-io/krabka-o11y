/// Returns the arithmetic mean of a non-empty `values` window.
///
/// This is the engine's `avg_over_time` fold, so the UDF and the engine agree
/// bit-for-bit. An empty window returns NaN.
pub(crate) fn over_time_mean(values: &[f64]) -> f64 {
    crate::engine::over_time_mean(values.iter().copied())
}
