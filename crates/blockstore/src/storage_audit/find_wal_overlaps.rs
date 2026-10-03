use super::{StorageFinding, StorageFindingKind, StorageInventory, StorageSignal};

/// Finds pairs of blocks that cover overlapping WAL offsets.
///
/// Two blocks of one signal, tenant, partition and lane whose inclusive
/// offset ranges meet and differ both hold the records of the overlap. Two
/// writers on one partition do that, and so does a replay after a crash that
/// cut a different window. Queries deduplicate the rows, so the kind is a
/// warning. Two blocks with the same range are one window written twice
/// under two keys and are not reported. A block with no offsets in its key,
/// such as a compacted block, takes no part.
pub fn find_wal_overlaps(
    inventory: &StorageInventory,
    signal: StorageSignal,
) -> Vec<StorageFinding> {
    let mut blocks: Vec<_> = inventory
        .blocks(signal)
        .filter_map(|(key, listed)| {
            let block = listed.object.block()?;
            let (first, last) = block.offsets?;
            Some((
                listed.object.tenant.as_deref(),
                block.partition,
                block.lane.as_str(),
                first,
                last,
                key.as_str(),
            ))
        })
        .collect();
    blocks.sort_unstable();
    let mut findings = Vec::new();
    for lane in blocks.chunk_by(|a, b| (a.0, a.1, a.2) == (b.0, b.1, b.2)) {
        for (at, (tenant, partition, _, first, last, key)) in lane.iter().enumerate() {
            for (_, _, _, other_first, other_last, other) in &lane[at + 1..] {
                if other_first > last {
                    break;
                }
                if (first, last) == (other_first, other_last) {
                    continue;
                }
                let partition = partition.map_or_else(|| "-".to_string(), |p| p.to_string());
                findings.push(StorageFinding::new(
                    StorageFindingKind::WalOverlap,
                    signal,
                    tenant.map(str::to_string),
                    *key,
                    format!(
                        "offsets {first}-{last} of partition {partition} overlap \
                         offsets {other_first}-{other_last} of `{other}`"
                    ),
                ));
            }
        }
    }
    findings
}
