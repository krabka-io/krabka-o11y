use super::{CompactionJob, input_key_fingerprint};

/// Names the span block a planned job writes.
///
/// The level is in the name because it is the first thing anyone reading a
/// bucket listing wants to know, and the fingerprint of the input keys is what
/// keeps two jobs over the same time range from colliding.
#[must_use]
pub fn planned_compacted_object_key(job: &CompactionJob) -> String {
    let (tenant, level, min_ts, max_ts) = (&job.tenant, job.output_level, job.min_ts, job.max_ts);
    format!(
        "traces/{tenant}/compacted/l{level}-{min_ts}-{max_ts}-{:016x}.parquet",
        input_key_fingerprint(&job.input_keys)
    )
}
