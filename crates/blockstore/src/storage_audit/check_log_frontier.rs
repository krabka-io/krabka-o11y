use super::{
    Arc, BTreeMap, LOG_FRONTIER_PATH, LogFrontierManifest, ObjectStore, StorageAuditError,
    StorageFinding, StorageFindingKind, StorageInventory, StorageSignal, read_small_object,
};

/// Compares the logs compaction frontier with the log blocks of each WAL
/// partition.
///
/// A frontier offset above the last offset of every retained block of its
/// partition is `stale_frontier`. The querier trusts the frontier to say
/// which WAL records are in blocks, so it hides records from the hot tail
/// that no block holds. A batch that the delete filters emptied does this
/// legally, which is why the kind is a warning.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence.
pub async fn check_log_frontier(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    if !inventory.contains(LOG_FRONTIER_PATH) {
        return Ok(Vec::new());
    }
    let Some(bytes) = read_small_object(store, LOG_FRONTIER_PATH).await? else {
        return Ok(Vec::new());
    };
    let unreadable = |kind, detail: String| {
        Ok(vec![StorageFinding::new(
            kind,
            StorageSignal::Logs,
            None,
            LOG_FRONTIER_PATH,
            detail,
        )])
    };
    let frontier: LogFrontierManifest = match serde_json::from_slice(&bytes) {
        Ok(frontier) => frontier,
        Err(error) => return unreadable(StorageFindingKind::UnreadableManifest, error.to_string()),
    };
    if frontier.version != LogFrontierManifest::VERSION {
        return unreadable(
            StorageFindingKind::UnsupportedFormat,
            format!(
                "compaction frontier manifest version {}, expected {}",
                frontier.version,
                LogFrontierManifest::VERSION
            ),
        );
    }
    let mut last_offsets: BTreeMap<i32, i64> = BTreeMap::new();
    for (_, listed) in inventory.blocks(StorageSignal::Logs) {
        if let Some(block) = listed.object.block()
            && let (Some(partition), Some((_, last))) = (block.partition, block.offsets)
        {
            let entry = last_offsets.entry(partition).or_insert(last);
            *entry = (*entry).max(last);
        }
    }
    Ok(frontier
        .partition_offsets
        .iter()
        .filter_map(|(partition, frontier_offset)| {
            let last = last_offsets.get(partition)?;
            (frontier_offset > last).then(|| {
                StorageFinding::new(
                    StorageFindingKind::StaleFrontier,
                    StorageSignal::Logs,
                    None,
                    LOG_FRONTIER_PATH,
                    format!(
                        "partition {partition} is compacted through offset {frontier_offset}, \
                         and its newest block ends at offset {last}"
                    ),
                )
            })
        })
        .collect())
}
