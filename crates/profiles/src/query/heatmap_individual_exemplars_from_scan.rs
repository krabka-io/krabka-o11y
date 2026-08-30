use super::{
    AsArray, BTreeMap, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_TOTAL_VALUE, ProfileError,
    heatmap_slot_timestamp, label_pairs, pb,
};

pub(crate) async fn heatmap_individual_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    labels: &[(String, String)],
    profile_id: &str,
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint} \
         ORDER BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        fingerprint = COL_FINGERPRINT,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let labels = label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::querier::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let timestamp = timestamps.value(row);
            let Some(slot_timestamp) =
                heatmap_slot_timestamp(start_ms, end_ms, time_buckets, timestamp)
            else {
                continue;
            };
            out.entry(slot_timestamp)
                .or_default()
                .push(pb::querier::v1::Exemplar {
                    timestamp,
                    profile_id: profile_id.to_string(),
                    span_id: String::new(),
                    value: totals.value(row),
                    labels: labels.clone(),
                });
        }
    }
    Ok(out)
}
