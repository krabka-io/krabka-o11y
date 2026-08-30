use super::*;

/// Deterministic block and index object keys for one partition compaction window.
// cargo-mutants: this is a thin partition-key wrapper over covered key helpers.
#[cfg_attr(test, mutants::skip)]
#[must_use]
pub fn compaction_partition_object_plan(
    tenant: &str,
    kind: MetricBlockKind,
    partition: PartitionIndex,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let block_key =
        compaction_partition_object_key(tenant, kind, partition, first_offset, last_offset);
    let index_key = compaction_index_key(&block_key);
    CompactionObjectPlan {
        block_key,
        index_key,
        first_offset,
        last_offset,
        row_count: 0,
    }
}
