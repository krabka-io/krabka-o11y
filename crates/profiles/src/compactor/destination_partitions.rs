use super::*;

/// Build the source-partition to destination-partition map for one input block.
///
/// The external partition scheme packs a per-block base in the high 32 bits and
/// a local partition id in the low 32 bits: `external = base | local`. That is
/// collision-free only while the local id fits the low 32 bits. After one
/// compaction of a block, its stored partitions already occupy the high bits. A
/// direct OR of a fresh base onto them then folds bits together, can alias
/// partitions across blocks, and trips the non-empty-destination reject of
/// `copy_partition_from`.
///
/// To stay safe across repeated compactions, this function first re-bases the
/// source partitions of each block to a dense local `0..n` range. The high-bit
/// base is then only ever OR-ed with small local ids. The caller sorts and
/// dedupes `source_partitions`, so the dense assignment is deterministic. The
/// function also uses checked arithmetic and returns an error instead of a
/// silent alias if a base or local id does not fit.
pub(crate) fn destination_partitions(
    block_idx: usize,
    source_partitions: &[u64],
) -> Result<BTreeMap<u64, u64>, ProfilesError> {
    let block_base = u64::try_from(block_idx + 1)
        .map_err(|err| ProfilesError::Block(format!("block index does not fit u64: {err}")))?
        .checked_shl(32)
        .ok_or_else(|| {
            ProfilesError::Block(format!("block base for index {block_idx} overflows u64"))
        })?;
    let mut map = BTreeMap::new();
    for (local, source) in source_partitions.iter().enumerate() {
        let local = u64::try_from(local).map_err(|err| {
            ProfilesError::Block(format!("local partition does not fit u64: {err}"))
        })?;
        if local >= 1 << 32 {
            return Err(ProfilesError::Block(format!(
                "local partition {local} does not fit the low 32 bits"
            )));
        }
        // `block_base` is a multiple of `1 << 32` and `local < 1 << 32`, so the
        // low bits are guaranteed clear and OR is equivalent to addition.
        map.insert(*source, block_base | local);
    }
    Ok(map)
}
