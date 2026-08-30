use super::*;

/// The assembled operator plan and the per-series labels that reattach label
/// sets to the projected rate values.
pub struct RateRangePlan {
    /// Session context with the leaf table registered. Its physical planner
    /// understands the custom operators, and its registry holds the rate UDFs.
    pub ctx: SessionContext,
    /// The `SeriesDivide -> SeriesNormalize -> RangeManipulate -> Projection`
    /// logical plan.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint. The caller uses them to assemble the
    /// result.
    pub labels_by_fp: std::collections::BTreeMap<SeriesFingerprint, Labels>,
}
