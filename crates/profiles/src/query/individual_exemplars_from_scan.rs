use krabka_pprof::call_site_profile_totals;

use super::{
    BTreeMap, IndividualProfile, ProfileError, Time, individual_exemplars_from_totals, pb,
    step_bucket_ms, types_label_pairs,
};

pub(crate) async fn individual_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[(String, String)],
    profile_id: &str,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return individual_exemplars_from_totals(
            scan,
            step,
            IndividualProfile { profile_id, labels },
        )
        .await;
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for (timestamp, value) in call_site_profile_totals(scan, call_sites).await? {
        out.entry(step_bucket_ms(timestamp, step))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp,
                profile_id: profile_id.to_string(),
                span_id: String::new(),
                trace_id: String::new(),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}
