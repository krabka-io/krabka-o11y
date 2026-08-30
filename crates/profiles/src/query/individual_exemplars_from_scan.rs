use super::*;

pub(crate) async fn individual_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[(String, String)],
    profile_id: &str,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return individual_exemplars_from_totals(scan, step, labels, profile_id).await;
    }
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} GROUP BY {timestamp}, {fingerprint}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut per_profile: BTreeMap<(i64, u64), i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(3).as_primitive::<UInt64Type>();
        let values = batch.column(4).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let frames = scan.symbols.resolve(partition, stacktrace_id);
            if frames_match_call_sites(&frames, call_sites) {
                *per_profile
                    .entry((timestamps.value(row), fingerprints.value(row)))
                    .or_default() += values.value(row);
            }
        }
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for ((timestamp, _fingerprint), value) in per_profile {
        out.entry(step_bucket_ms(timestamp, step))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp,
                profile_id: profile_id.to_string(),
                span_id: String::new(),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}
