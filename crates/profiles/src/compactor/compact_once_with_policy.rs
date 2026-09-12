use super::{
    Arc, CompactionPass, CompactionPolicy, DownsamplePolicy, ObjectStore, ProfileIndex,
    ProfilesError, compact_blocks_with_policy, compacted_key, plan_compactions,
};

/// Runs one compaction pass over the whole index.
///
/// A pass plans and executes; it does not loop. Running it again picks up
/// where this one left off, one rung further up the ladder, and eventually
/// plans nothing.
///
/// The pass leaves the objects of the blocks it retired in place and names
/// them in [`CompactionPass::retired_keys`]. Deleting them is the caller's,
/// because a reader resolves a block key through the index and the index that
/// no longer names them is not durable until the caller saves it.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_once_with_policy(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    policy: CompactionPolicy,
    downsample: Option<DownsamplePolicy>,
) -> Result<CompactionPass, ProfilesError> {
    let jobs = plan_compactions(index, policy);
    let mut pass = CompactionPass::default();
    for job in jobs {
        let output_key = compacted_key(&job);
        pass.outputs.push(
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
        pass.retired_keys.extend(job.input_keys);
    }
    Ok(pass)
}
