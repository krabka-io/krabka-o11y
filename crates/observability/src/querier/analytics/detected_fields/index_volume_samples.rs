use super::{
    BTreeMap, ByteSizeExt, Labels, QuerierState, StreamPlan, VolumeParams, plan_hot_tail_records,
    sample_time_bucket, volume_metrics_for_labels,
};

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

    // A record still in the WAL counts its line toward the step it falls in.
    for record in plan_hot_tail_records(state, plan) {
        let sample_time = params
            .step
            .filter(|step| *step > 0)
            .map_or(record.timestamp_ns.max(plan.time_range.start_ns), |step| {
                sample_time_bucket(record.timestamp_ns, plan.time_range.start_ns, step)
            });
        let bytes = u64::try_from(record.line.len()).unwrap_or(u64::MAX);
        for metric in volume_metrics_for_labels(&record.labels, params) {
            let samples = volumes.entry(metric).or_default();
            let sample = samples.entry(sample_time).or_default();
            *sample = sample.saturating_add(bytes);
        }
    }
    volumes
}
