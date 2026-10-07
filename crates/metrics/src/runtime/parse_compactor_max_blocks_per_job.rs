/// Parses the compaction fan-in cap, which is at least two blocks.
///
/// A job of one block rewrites that block and leaves the same rows behind,
/// which is the loop a planner must not enter.
/// [`CompactionPolicy`](krabka_blockstore::CompactionPolicy) clamps the value
/// anyway; refusing it here tells the operator instead of silently correcting
/// them.
pub(crate) fn parse_compactor_max_blocks_per_job(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed >= 2 => Ok(parsed),
        Ok(_) => Err("value must be at least 2".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
