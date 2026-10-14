use super::{StorageFinding, StorageFindingKind, StorageInventory, StorageSignal};

struct BlockOverlapRecord {
    tenant: Option<String>,
    partition: Option<i32>,
    lane: String,
    offsets: crate::OffsetRange,
    object_key: String,
}

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
        .filter_map(|entry| {
            let key = entry.key;
            let listed = entry.listed;
            let block = listed.object.block()?;
            Some(BlockOverlapRecord {
                tenant: listed.object.tenant.clone(),
                partition: block.partition,
                lane: block.lane.clone(),
                offsets: block.offsets?,
                object_key: key.clone(),
            })
        })
        .collect();
    blocks.sort_by(|left, right| {
        left.tenant
            .cmp(&right.tenant)
            .then_with(|| left.partition.cmp(&right.partition))
            .then_with(|| left.lane.cmp(&right.lane))
            .then_with(|| left.offsets.min.cmp(&right.offsets.min))
            .then_with(|| left.offsets.max.cmp(&right.offsets.max))
            .then_with(|| left.object_key.cmp(&right.object_key))
    });
    let mut findings = Vec::new();
    for lane in blocks.chunk_by(|left, right| {
        left.tenant == right.tenant && left.partition == right.partition && left.lane == right.lane
    }) {
        for at in 0..lane.len() {
            let block = &lane[at];
            for other in &lane[at + 1..] {
                if other.offsets.min > block.offsets.max {
                    break;
                }
                if block.offsets == other.offsets {
                    continue;
                }
                let partition = block
                    .partition
                    .map_or_else(|| "-".to_string(), |p| p.to_string());
                findings.push(StorageFinding::new(
                    StorageFindingKind::WalOverlap,
                    signal,
                    block.tenant.clone(),
                    &block.object_key,
                    format!(
                        "offsets {}-{} of partition {partition} overlap offsets {}-{} of `{}`",
                        block.offsets.min,
                        block.offsets.max,
                        other.offsets.min,
                        other.offsets.max,
                        other.object_key,
                    ),
                ));
            }
        }
    }
    findings
}
