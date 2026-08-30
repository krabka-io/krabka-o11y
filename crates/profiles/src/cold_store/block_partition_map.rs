use super::*;

/// Build the dense per-block partition map that the cold read path uses.
///
/// `stored_partitions` are the partitions of this block as the index records
/// them. They are already in the high bits if the block was compacted. This
/// function re-bases them to a dense local `0..n` range and ORs them with the
/// per-block high-bit base. The external keys are then unique even when a read
/// covers several already-compacted blocks. The map is
/// `stored_partition -> base | local_id`.
pub(crate) fn block_partition_map(
    block_idx: usize,
    stored_partitions: &[u64],
) -> Result<BTreeMap<u64, u64>, ProfileError> {
    let base = u64::try_from(block_idx + 1)
        .map_err(|err| ProfileError::Store(format!("block index does not fit u64: {err}")))?
        .checked_shl(32)
        .ok_or_else(|| {
            ProfileError::Store(format!("block base for index {block_idx} overflows u64"))
        })?;
    let mut sorted = stored_partitions.to_vec();
    if sorted.is_empty() {
        sorted.push(STACKTRACE_PARTITION);
    }
    sorted.sort_unstable();
    sorted.dedup();
    let mut map = BTreeMap::new();
    for (local, stored) in sorted.into_iter().enumerate() {
        let local = u64::try_from(local).map_err(|err| {
            ProfileError::Store(format!("local partition does not fit u64: {err}"))
        })?;
        if local >= 1 << 32 {
            return Err(ProfileError::Store(format!(
                "local partition {local} does not fit the low 32 bits"
            )));
        }
        map.insert(stored, base | local);
    }
    Ok(map)
}
