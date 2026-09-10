use super::{
    Arc, Cli, CompactionPolicy, DownsamplePolicy, ObjectStore, ProfileIndex,
    compact_once_with_policy,
};

/// Loads the index, compacts what the policy asks for, and publishes the
/// result. Returns how many replacement blocks the pass wrote.
///
/// # Errors
/// Returns an error when the index cannot be loaded or saved, or when a
/// compaction job fails.
pub(crate) async fn run_compaction_pass(
    store: &Arc<dyn ObjectStore>,
    index_key: &str,
    cli: &Cli,
    policy: CompactionPolicy,
    downsample: Option<DownsamplePolicy>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
        store,
        index_key,
        cli.index_snapshot_max,
    )
    .await?;
    let metas = compact_once_with_policy(store, &mut index, policy, downsample).await?;
    // A pass that planned nothing has nothing to publish. Saving anyway would
    // burn a snapshot generation every tick and evict the retained history
    // that a reader falls back on.
    if metas.is_empty() {
        return Ok(0);
    }
    index
        .save_latest_snapshot_with_retain(store, index_key, cli.index_snapshot_retain)
        .await?;
    Ok(metas.len())
}
