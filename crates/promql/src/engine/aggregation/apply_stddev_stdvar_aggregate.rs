use super::{InstantSample, AggregateOp, LabelModifier, apply_simple_aggregate};

/// Shared `stddev(v)` / `stdvar(v)` core over an already-evaluated float-only
/// instant vector.
///
/// This function backs both the interpreter and the operator path. The
/// interpreter reaches it through the general
/// `PromqlEngine::eval_instant_aggregate` loop, which builds the same
/// [`AggregateState`] and calls the same [`AggregateOp::finish`]. The function
/// groups the float samples by the `by`/`without` label set, accumulates each
/// group's running sum, sum of squares, and count, and returns the population
/// standard deviation (`Stddev`) or the variance (`Stdvar`) per group. `op` must
/// be [`AggregateOp::Stddev`] or [`AggregateOp::Stdvar`]. This function ignores
/// histogram samples exactly as the interpreter ignores them for these ops. The
/// operator path feeds only float-only inputs, so no histogram sample appears in
/// practice.
pub(crate) fn apply_stddev_stdvar_aggregate(
    samples: Vec<InstantSample>,
    op: AggregateOp,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Vec<InstantSample> {
    debug_assert!(
        matches!(op, AggregateOp::Stddev | AggregateOp::Stdvar),
        "apply_stddev_stdvar_aggregate requires a stddev/stdvar op"
    );
    // `stddev`/`stdvar` are `ignores_histograms` ops, so the shared simple-
    // aggregate kernel skips histogram samples (its `op.ignores_histograms()`
    // no-op branch) exactly as this routine used to, and never hits the
    // unreachable error branch. Delegating keeps the interpreter and operator
    // param paths sharing one core.
    apply_simple_aggregate(samples, op, modifier, time_ms)
        .expect("stddev/stdvar ignore histograms, so the kernel is infallible here")
}
