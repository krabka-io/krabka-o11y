use super::*;

/// Folds a non-empty window to its `min` or `max` and ignores NaN.
///
/// The fold seeds with the first sample, NaN included, then replaces the
/// running value under [`Extremum::should_replace`]. The result is NaN only
/// when every sample is NaN. This matches Prometheus, the engine's
/// `over_time_sample_from_series`, and the `prom_min`/`prom_max` aggregate UDAF.
pub(crate) fn fold_extremum(values: &[f64], extremum: Extremum) -> f64 {
    let mut running = values[0];
    for &candidate in &values[1..] {
        if extremum.should_replace(running, candidate) {
            running = candidate;
        }
    }
    running
}
