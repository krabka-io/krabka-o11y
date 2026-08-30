use super::*;

pub(crate) fn next_snapshot_key(key: &str) -> Result<String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            BlockStoreError::InvalidBlock(format!("system clock before epoch: {err}"))
        })?;
    let counter = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(format!(
        "{}/{:020}-{:016}.json",
        index_snapshot_prefix_for_key(key),
        elapsed.as_nanos(),
        counter
    ))
}
