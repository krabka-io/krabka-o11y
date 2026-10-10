use super::{
    BTreeMap, BucketExemplars, ExemplarRow, ExemplarSource, ProfileError, bucket_exemplars,
    heatmap_slot_timestamp, label_pairs, pb,
};

pub(crate) async fn heatmap_span_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    labels: &[(String, String)],
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let labels = label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::SpansBySampleValue,
        bucket: |timestamp| {
            heatmap_slot_timestamp(start_ms.saturating_add(step_ms), end_ms, step_ms, timestamp)
        },
        exemplar: |row: ExemplarRow| pb::querier::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: String::new(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: labels.clone(),
        },
    })
    .await
}
