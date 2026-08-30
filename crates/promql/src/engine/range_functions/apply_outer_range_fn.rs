use super::*;

/// Applies an [`OuterRangeFn`] over an evaluated range vector.
///
/// This function returns the instant vector at `time_ms`. It is the one shared
/// implementation of every range and `*_over_time` function's per-series fold.
/// The interpreter and the planner's subquery path both route through it, so
/// they cannot diverge.
pub(crate) fn apply_outer_range_fn(
    range: RangeEval,
    outer: OuterRangeFn,
    time_ms: i64,
) -> Vec<InstantSample> {
    range
        .series
        .into_iter()
        .filter_map(|series| {
            outer_range_sample_from_series(
                &series,
                range.end_ms,
                range.range,
                outer,
                range.modifier,
            )
            .map(|(labels, value)| InstantSample {
                labels,
                ts_ms: time_ms,
                value,
            })
        })
        .collect()
}
