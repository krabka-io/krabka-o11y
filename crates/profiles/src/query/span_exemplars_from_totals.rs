use super::{
    Array, AsArray, BTreeMap, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_SPAN_ID,
    PCOL_TOTAL_VALUE, ProfileError, Time, UInt64Type, pb, step_bucket_ms, types_label_pairs,
};

pub(crate) async fn span_exemplars_from_totals(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[(String, String)],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
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
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let totals = batch.column(3).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
            let timestamp = timestamps.value(row);
            out.entry(step_bucket_ms(timestamp, step))
                .or_default()
                .push(pb::types::v1::Exemplar {
                    timestamp,
                    profile_id: String::new(),
                    span_id: format!("{:x}", span_ids.value(row)),
                    value: totals.value(row),
                    labels: label_pairs.clone(),
                });
        }
    }
    Ok(out)
}
