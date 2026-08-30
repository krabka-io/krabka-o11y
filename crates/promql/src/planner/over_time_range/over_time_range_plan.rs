use super::*;

/// The assembled operator plan with the per-series labels.
///
/// The labels reattach label sets to the projected `*_over_time` values.
pub struct OverTimeRangePlan {
    /// Session context with the leaf table registered. Its physical planner
    /// knows the custom operators and its registry holds the `*_over_time` UDFs.
    pub ctx: SessionContext,
    /// The `SeriesDivide -> SeriesNormalize -> RangeManipulate -> Projection`
    /// logical plan.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint. The assembler builds the result from them.
    pub labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
}
