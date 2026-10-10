use super::{
    BTreeMap, BucketExemplars, ExemplarRow, ExemplarSource, IndividualProfile, ProfileError, Time,
    bucket_exemplars, pb, step_bucket_ms, types_label_pairs,
};

pub(crate) async fn individual_exemplars_from_totals(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    profile: IndividualProfile<'_>,
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    let IndividualProfile { profile_id, labels } = profile;
    let label_pairs = types_label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::Profiles,
        bucket: |timestamp| Some(step_bucket_ms(timestamp, step)),
        exemplar: |row: ExemplarRow| pb::types::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: profile_id.to_string(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: label_pairs.clone(),
        },
    })
    .await
}
