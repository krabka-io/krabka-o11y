use super::*;

/// Folds a non-empty sample window for `min_over_time` or `max_over_time`.
///
/// The fold ignores NaN. It seeds with the first sample, NaN included, then
/// replaces the running value under [`ExtremumKind::should_replace`]. The
/// result is NaN only when every sample is NaN. This matches Prometheus, the
/// `*_over_time` UDF, and the `min`/`max` aggregate.
pub(crate) fn fold_over_time_extremum(samples: &[(i64, f64)], extremum: ExtremumKind) -> f64 {
    let mut running = samples[0].1;
    for (_, candidate) in &samples[1..] {
        if extremum.should_replace(running, *candidate) {
            running = *candidate;
        }
    }
    running
}
