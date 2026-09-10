use super::{
    Arc, BlockMeta, CompactionPolicy, DownsamplePolicy, ObjectStore, ProfileIndex, ProfilesError,
    compact_blocks_with_policy, compacted_key, plan_compactions,
};

/// Runs one compaction pass over the whole index.
///
/// A pass plans and executes; it does not loop. Running it again picks up
/// where this one left off, one rung further up the ladder, and eventually
/// plans nothing.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_once_with_policy(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    policy: CompactionPolicy,
    downsample: Option<DownsamplePolicy>,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let jobs = plan_compactions(index, policy);
    let mut metas = Vec::new();
    for job in jobs {
        let output_key = compacted_key(&job);
        metas.push(
            compact_blocks_with_policy(
                store,
                index,
                &job.tenant,
                &job.input_keys,
                &output_key,
                downsample,
            )
            .await?,
        );
    }
    Ok(metas)
}
