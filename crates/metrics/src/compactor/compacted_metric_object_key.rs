use super::{CompactionJob, MetricBlockKind, escape_object_path_segment, input_key_fingerprint};

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
    let (level, min_ts, max_ts) = (job.output_level, job.min_ts, job.max_ts);
    let tenant = escape_object_path_segment(&job.tenant);
    let kind = kind.object_path();
    format!(
        "metrics/{tenant}/{kind}/compacted/l{level}-{min_ts}-{max_ts}-{:016x}.parquet",
        input_key_fingerprint(&job.input_keys)
    )
}
