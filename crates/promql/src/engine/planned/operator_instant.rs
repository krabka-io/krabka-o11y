use super::{SessionContext, LogicalPlan, BTreeMap, SeriesFingerprint, Labels, InstantShape};

/// The executable payload of `PlannedInstant::Operator`.
pub(crate) struct OperatorInstant {
    /// Session context whose physical planner understands the custom operators.
    /// It also holds the rate UDFs and the registered inner leaf table.
    pub(crate) ctx: SessionContext,
    /// The fully-lowered logical plan to execute.
    pub(crate) plan: LogicalPlan,
    /// Series labels keyed by fingerprint, used when the selector and rate shapes
    /// assemble their result. The aggregate and scalar-math shapes read labels
    /// straight from the batch and leave this map empty.
    pub(crate) labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
    /// How to read the output batches into an instant vector.
    pub(crate) shape: InstantShape,
}
