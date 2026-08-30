use super::*;

pub(crate) fn source_partitions(index: &ProfileIndex, block_key: &str) -> Vec<u64> {
    let mut partitions = index.stacktrace_partitions(block_key);
    if partitions.is_empty() {
        return vec![STACKTRACE_PARTITION];
    }
    // Sort + dedup so the dense local re-basing in `destination_partitions` is
    // deterministic regardless of the order the index recorded partitions in.
    partitions.sort_unstable();
    partitions.dedup();
    partitions
}
