use super::{
    BTreeMap, BucketExemplars, ExemplarRow, ExemplarSource, HeatmapSlotsMillis, IndividualProfile,
    ProfileError, bucket_exemplars, heatmap_slot_timestamp, label_pairs, pb,
};

pub(crate) async fn heatmap_individual_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    slots: HeatmapSlotsMillis,
    profile: IndividualProfile<'_>,
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let HeatmapSlotsMillis {
        start: start_ms,
        end: end_ms,
        step: step_ms,
    } = slots;
    let IndividualProfile { profile_id, labels } = profile;
    let labels = label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::Profiles,
        bucket: |timestamp| {
            heatmap_slot_timestamp(start_ms.saturating_add(step_ms), end_ms, step_ms, timestamp)
        },
        exemplar: |row: ExemplarRow| pb::querier::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: profile_id.to_string(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: labels.clone(),
        },
    })
    .await
}
