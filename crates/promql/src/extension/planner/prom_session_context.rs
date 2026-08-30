use super::{
    Arc, PromQueryPlanner, SessionContext, SessionStateBuilder, register_aggregate_udafs,
    register_over_time_udfs, register_rate_udfs, register_scalar_math_udfs,
};

/// Builds a [`SessionContext`] for the custom `PromQL` operator nodes.
///
/// The physical planner of the returned context handles [`SeriesDivide`],
/// [`SeriesNormalize`], [`InstantManipulate`], and [`RangeManipulate`]. Its
/// function registry holds the rate-family, `*_over_time`, and per-row
/// scalar-math `ScalarUDF`s. The registry also holds the NaN-ignoring
/// `prom_min` and `prom_max` aggregate UDAFs. A range-function, scalar-math, or
/// `min`/`max` aggregation can then lower onto them.
#[must_use]
pub fn prom_session_context() -> SessionContext {
    // Pin single-partition execution. The custom PromQL operator chain
    // ([`SeriesNormalize`] / [`SeriesDivide`] / [`InstantManipulate`] /
    // [`RangeManipulate`]) assumes its input arrives as one ordered partition
    // (each series contiguous, sorted by fingerprint then timestamp). With the
    // default `target_partitions` = CPU count, DataFusion's `EnforceDistribution`
    // rule inserts a repartition ahead of the operator chain / aggregate that
    // scatters a series across partitions, silently producing wrong results —
    // reproduced deterministically at `target_partitions` in `2..=6` (e.g.
    // `COUNT(m) BY (job)` collapsing 4 series to 2 on the 2-4 core CI runners,
    // while a high-core dev box at 32 partitions happens to dodge it). Per-query
    // parallelism comes from the query-frontend's shard fan-out, not from
    // DataFusion intra-query partitioning, so pinning one partition costs nothing.
    let config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_query_planner(Arc::new(PromQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    register_rate_udfs(&ctx);
    register_over_time_udfs(&ctx);
    register_scalar_math_udfs(&ctx);
    register_aggregate_udafs(&ctx);
    ctx
}
