use super::*;

pub(crate) async fn heatmap_span_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    labels: &[(String, String)],
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, MAX({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span} \
         ORDER BY {timestamp}, {fingerprint}, {span}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        total = PCOL_TOTAL_VALUE,
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
    let labels = label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::querier::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let totals = batch.column(3).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
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
                    profile_id: String::new(),
                    span_id: format!("{:x}", span_ids.value(row)),
                    value: totals.value(row),
                    labels: labels.clone(),
                });
        }
    }
    Ok(out)
}
