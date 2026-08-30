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
