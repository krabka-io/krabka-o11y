use std::sync::Arc;

use super::{HashMap, LeafMemo, StepGrid};
use crate::engine::subquery_grid::SubqueryGrid;

/// The state behind [`StepVectorCache`](super::StepVectorCache).
pub(crate) struct StepVectorCacheInner {
    /// The range query's step grid. Every memoized leaf is indexed by it.
    pub(crate) grid: StepGrid,
    /// Points kept across the leaves and subquery unions, against the size budget.
    pub(crate) points: usize,
    /// Each leaf's whole-grid result, keyed by the leaf's rendered `PromQL` text
    /// plus its fold.
    pub(crate) leaves: HashMap<String, LeafMemo>,
    /// A declined union stays declined for the rest of this range evaluation.
    pub(crate) subqueries: HashMap<String, Option<Arc<SubqueryGrid>>>,
}
