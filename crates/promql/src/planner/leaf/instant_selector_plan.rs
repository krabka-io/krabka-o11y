use super::{Labels, LogicalPlan, SeriesFingerprint, SessionContext};

/// The assembled operator plan plus the per-series labels needed to reattach
/// label sets to the selected samples.
pub struct InstantSelectorPlan {
    /// Session context whose physical planner understands the custom operators
    /// and where the leaf table is registered.
    pub ctx: SessionContext,
    /// The `SeriesDivide -> SeriesNormalize -> InstantManipulate` logical plan.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for assembling the result.
    pub labels_by_fp: std::collections::BTreeMap<SeriesFingerprint, Labels>,
}
