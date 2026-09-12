use super::{
    CompactionCandidate, CompactionIndexManifest, CompactionPolicy, MetricBlockKind,
    MetricCompactionJob, plan_compactions,
};

/// Plans the merges the metrics index is due, under `policy`.
///
/// The shared planner groups by tenant, level and time bucket. It knows nothing
/// about payload kinds, so this runs it once per mergeable kind: a job holds
/// blocks of one kind, and the kind is what fixes the output schema.
///
/// Only the kinds [`MetricBlockKind::is_mergeable`] admits are offered to the
/// planner at all, so an exemplar, metadata or clock-reading block is never a
/// compaction input. See that method for why each is left out.
///
/// See [`plan_compactions`] for why repeated plan-and-apply terminates.
#[must_use]
pub fn plan_metric_compactions(
    manifests: &[CompactionIndexManifest],
    policy: CompactionPolicy,
) -> Vec<MetricCompactionJob> {
    let kinds = [
        MetricBlockKind::Float,
        MetricBlockKind::NativeHistograms,
        MetricBlockKind::Exemplars,
        MetricBlockKind::Metadata,
        MetricBlockKind::ClockReadings,
    ];
    kinds
        .into_iter()
        .filter(|kind| kind.is_mergeable())
        .flat_map(|kind| {
            let candidates: Vec<CompactionCandidate> = manifests
                .iter()
                .filter(|manifest| manifest.kind == kind)
                .map(|manifest| CompactionCandidate {
                    tenant: manifest.tenant.clone(),
                    object_key: manifest.block_key.clone(),
                    min_ts: manifest.min_ts,
                    max_ts: manifest.max_ts,
                    row_count: manifest.row_count,
                    level: manifest.level,
                })
                .collect();
            plan_compactions(&candidates, policy)
                .into_iter()
                .map(move |job| MetricCompactionJob { kind, job })
        })
        .collect()
}
