use super::{HashMap, LeafMemo, StepGrid};

/// The state behind [`StepVectorCache`](super::StepVectorCache).
pub(crate) struct StepVectorCacheInner {
    /// The range query's step grid. Every memoized leaf is indexed by it.
    pub(crate) grid: StepGrid,
    /// Points already memoized across every leaf, against the size budget.
    pub(crate) points: usize,
    /// Each leaf's whole-grid result, keyed by the leaf's rendered `PromQL` text
    /// plus its fold.
    pub(crate) leaves: HashMap<String, LeafMemo>,
}
