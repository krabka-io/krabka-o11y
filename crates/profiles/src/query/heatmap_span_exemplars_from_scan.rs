use super::{
    Array, AsArray, BTreeMap, BinaryArray, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_SPAN_ID,
    PCOL_TOTAL_VALUE, PCOL_TRACE_ID, ProfileError, UInt64Type, heatmap_slot_timestamp, label_pairs,
    pb, span_id_hex_from_u64,
};

pub(crate) async fn heatmap_span_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    labels: &[(String, String)],
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, {trace}, MAX({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span}, {trace} \
         ORDER BY {timestamp}, {fingerprint}, {span}, {trace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        trace = PCOL_TRACE_ID,
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
        let trace_ids = batch.column(3).as_binary::<i32>() as &BinaryArray;
        let totals = batch.column(4).as_primitive::<Int64Type>();
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
                    span_id: span_id_hex_from_u64(span_ids.value(row)),
                    trace_id: if trace_ids.is_null(row) {
                        String::new()
                    } else {
                        hex::encode(trace_ids.value(row))
                    },
                    value: totals.value(row),
                    labels: labels.clone(),
                });
        }
    }
    Ok(out)
}
