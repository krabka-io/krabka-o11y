use super::*;

/// Deterministic object key for one tenant/kind/WAL offset compaction window.
///
/// The key is a pure function of `(tenant, kind, first_offset, last_offset)`, so
/// a re-compaction of the *same* offset range writes the *same* object key as an
/// idempotent overwrite. The accumulate-then-flush loop does NOT guarantee that
/// the same range forms again after a crash before a commit. The flushed window
/// depends on poll batching and the age timer, so a re-run can write the same
/// records under a *different* key. That is at-least-once delivery and not
/// byte-identical idempotency. Offset-overlapping duplicate blocks carry
/// identical `(series, ts, value)` rows, and the timestamp-keyed `PromQL`
/// operator engine deduplicates them at query time, so they do not
/// double-count.
///
/// The tenant is one path segment, escaped with
/// [`krabka_blockstore::escape_object_path_segment`]. A tenant name therefore
/// cannot add a segment or step out of the `metrics/` prefix, and the object
/// store keeps the key exactly as this function writes it.
#[must_use]
pub fn compaction_object_key(
    tenant: &str,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> String {
    format!(
        "metrics/{}/{}/{:020}-{:020}.parquet",
        escape_object_path_segment(tenant),
        kind.object_path(),
        first_offset,
        last_offset
    )
}

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

/// Deterministic object plan plus row-count evidence for one encoded block kind.
#[must_use]
pub fn compaction_object_plan_for_rows(
    rows: &TenantCompactionRows,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let mut plan = compaction_object_plan(&rows.tenant, kind, first_offset, last_offset);
    plan.row_count = match kind {
        MetricBlockKind::Float => rows.float_rows.len(),
        MetricBlockKind::NativeHistograms => rows.histogram_rows.len(),
        MetricBlockKind::Exemplars => rows.exemplar_rows.len(),
        MetricBlockKind::Metadata => rows.metadata_rows.len(),
        MetricBlockKind::ClockReadings => rows.clock_rows.len(),
    };
    plan
}

/// Deterministic object key for one tenant/kind/WAL partition/offset window.
///
/// The tenant is one escaped path segment, as in [`super::compaction_object_key`].
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

/// Names the metric block a planned merge writes.
///
/// The level is in the name because it is the first thing anyone reading a
/// bucket listing wants to know, and the fingerprint of the input keys is what
/// keeps two jobs over the same time range from colliding.
///
/// `compacted` sits under the kind segment, so a listing separates the
/// offset-keyed blocks a block builder wrote from the merged blocks a
/// compactor wrote. That matters because a reader tells a manifest from a block
/// by the `.index` extension alone, and nothing else in the key says which job
/// produced the object.
///
/// The tenant is one path segment, escaped as
/// [`compaction_object_key`](super::compaction_object_key) escapes it, so a
/// tenant name cannot add a segment or step out of the `metrics/` prefix.
#[must_use]
pub fn compacted_metric_object_key(job: &CompactionJob, kind: MetricBlockKind) -> String {
    let level = job.output_level;
    let min_ts = job.min_ts;
    let max_ts = job.max_ts;
    let tenant = escape_object_path_segment(&job.tenant);
    let kind = kind.object_path();
    format!(
        "metrics/{tenant}/{kind}/compacted/l{level}-{min_ts}-{max_ts}-{:016x}.parquet",
        input_key_fingerprint(&job.input_keys)
    )
}
