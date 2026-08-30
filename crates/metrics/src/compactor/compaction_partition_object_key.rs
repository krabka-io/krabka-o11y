use super::{MetricBlockKind, PartitionIndex, escape_object_path_segment};

/// Deterministic object key for one tenant/kind/WAL partition/offset window.
#[must_use]
pub fn compaction_partition_object_key(
    tenant: &str,
    kind: MetricBlockKind,
    partition: PartitionIndex,
    first_offset: i64,
    last_offset: i64,
) -> String {
    format!(
        "metrics/{}/{}/partition={:010}/{:020}-{:020}.parquet",
        escape_object_path_segment(tenant),
        kind.object_path(),
        partition.get(),
        first_offset,
        last_offset
    )
}
