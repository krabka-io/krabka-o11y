use super::{CompactionJob, escape_object_path_segment, input_key_fingerprint};

/// Names the block a job writes.
///
/// The level is in the name because it is the first thing anyone reading a
/// bucket listing wants to know, and the fingerprint of the input keys is what
/// keeps two jobs over the same time range from colliding. The tenant goes
/// through [`escape_object_path_segment`], as it does in
/// [`crate::blockbuilder::object_key`], so a compacted block sits under the
/// same tenant segment as its inputs.
pub(crate) fn compacted_key(job: &CompactionJob) -> String {
    let tenant = escape_object_path_segment(&job.tenant);
    let (level, min_ts, max_ts) = (job.output_level, job.min_ts, job.max_ts);
    format!(
        "blocks/{tenant}/compacted/l{level}-{min_ts}-{max_ts}-{:016x}.parquet",
        input_key_fingerprint(&job.input_keys)
    )
}
