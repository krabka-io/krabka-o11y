use super::*;

#[must_use]
pub fn plan_compactions(index: &ProfileIndex, max_blocks_per_job: usize) -> Vec<CompactionJob> {
    let max_blocks_per_job = max_blocks_per_job.max(2);
    let mut by_tenant: BTreeMap<String, Vec<BlockMeta>> = BTreeMap::new();
    for block in index.all_blocks() {
        by_tenant
            .entry(block.tenant.clone())
            .or_default()
            .push(block);
    }
    let mut jobs = Vec::new();
    for (tenant, mut blocks) in by_tenant {
        blocks.sort_by(|left, right| {
            left.min_ts
                .cmp(&right.min_ts)
                .then_with(|| left.max_ts.cmp(&right.max_ts))
                .then_with(|| left.object_key.cmp(&right.object_key))
        });
        for chunk in blocks.chunks(max_blocks_per_job) {
            if chunk.len() < 2 {
                continue;
            }
            let input_keys = chunk
                .iter()
                .map(|block| block.object_key.clone())
                .collect::<Vec<_>>();
            jobs.push(CompactionJob {
                tenant: tenant.clone(),
                output_key: compacted_key(&tenant, chunk, &input_keys),
                input_keys,
            });
        }
    }
    jobs
}
