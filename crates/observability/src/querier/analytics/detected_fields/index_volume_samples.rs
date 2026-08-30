use super::*;

pub(crate) fn index_volume_samples(
    state: &QuerierState,
    tenant: &str,
    plan: &StreamPlan,
    params: &VolumeParams,
) -> BTreeMap<Labels, BTreeMap<i64, u64>> {
    let mut volumes = BTreeMap::<Labels, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let matching_fingerprints = block
            .fingerprints
            .iter()
            .filter(|fingerprint| plan.fingerprints.contains(fingerprint))
            .copied()
            .collect::<Vec<_>>();
        if matching_fingerprints.is_empty() {
            continue;
        }

        let sample_time = block.key.time_range.start_ns.max(plan.time_range.start_ns);
        for fingerprint in matching_fingerprints {
            let Some(labels) = state.label_index.labels_for(tenant, fingerprint) else {
                continue;
            };
            for metric in volume_metrics_for_labels(labels, params) {
                let samples = volumes.entry(metric).or_default();
                let sample = samples.entry(sample_time).or_default();
                *sample = sample.saturating_add(block.size.bytes_u64());
            }
        }
    }
    volumes
}
