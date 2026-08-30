use super::{
    Arc, BlockMeta, DownsamplePolicy, ObjectStore, ProfileIndex, ProfilesError,
    compact_blocks_with_policy, plan_compactions,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_once_with_policy(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    max_blocks_per_job: usize,
    downsample: Option<DownsamplePolicy>,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let jobs = plan_compactions(index, max_blocks_per_job);
    let mut metas = Vec::new();
    for job in jobs {
        metas.push(
            compact_blocks_with_policy(
                store,
                index,
                &job.tenant,
                &job.input_keys,
                &job.output_key,
                downsample,
            )
            .await?,
        );
    }
    Ok(metas)
}
