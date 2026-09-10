use super::BlockLevel;

/// One block offered to [`super::plan_compactions`].
///
/// Every signal reduces its own block record to this, so the planner is the
/// same code for spans, profiles and series.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionCandidate {
    pub tenant: String,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    /// Rows in the block, or `0` when the index does not record one.
    pub row_count: usize,
    pub level: BlockLevel,
}
