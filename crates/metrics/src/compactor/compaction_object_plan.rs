use super::{MetricBlockKind, compaction_object_key, compaction_index_key};

/// Deterministic object names for one compacted block and its index sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionObjectPlan {
    pub block_key: String,
    pub index_key: String,
    pub first_offset: i64,
    pub last_offset: i64,
    pub row_count: usize,
}

/// Deterministic block and index object keys for one compaction window.
// cargo-mutants: covered by `compaction_object_plan_pairs_block_and_index_keys`.
#[cfg_attr(test, mutants::skip)]
#[must_use]
pub fn compaction_object_plan(
    tenant: &str,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let block_key = compaction_object_key(tenant, kind, first_offset, last_offset);
    let index_key = compaction_index_key(&block_key);
    CompactionObjectPlan {
        block_key,
        index_key,
        first_offset,
        last_offset,
        row_count: 0,
    }
}
