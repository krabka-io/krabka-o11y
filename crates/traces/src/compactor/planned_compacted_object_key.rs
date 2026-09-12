use super::{
    CompactionJob, TRACE_BLOCK_OBJECT_PREFIX, escape_object_path_segment, input_key_fingerprint,
};

/// Names the span block a planned job writes.
///
/// The level is in the name because it is the first thing anyone reading a
/// bucket listing wants to know, and the fingerprint of the input keys is what
/// keeps two jobs over the same time range from colliding. The tenant is one
/// path segment, escaped as [`crate::blockbuilder::object_key`] escapes it.
#[must_use]
pub fn planned_compacted_object_key(job: &CompactionJob) -> String {
    let (level, min_ts, max_ts) = (job.output_level, job.min_ts, job.max_ts);
    let tenant = escape_object_path_segment(&job.tenant);
    format!(
        "{TRACE_BLOCK_OBJECT_PREFIX}/{tenant}/compacted/l{level}-{min_ts}-{max_ts}-{:016x}.parquet",
        input_key_fingerprint(&job.input_keys)
    )
}
