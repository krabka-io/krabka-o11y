use super::{
    Array, AsArray, BTreeMap, BinaryArray, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_SPAN_ID,
    PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TRACE_ID, PCOL_VALUE, ProfileError, Time,
    UInt64Type, frames_match_call_sites, pb, span_exemplars_from_totals, span_id_hex_from_u64,
    step_bucket_ms, types_label_pairs,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SpanKey {
    timestamp: i64,
    fingerprint: u64,
    span_id: u64,
    trace_id: Option<Vec<u8>>,
}

pub(crate) async fn span_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[krabka_pprof::ProfileLabel],
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return span_exemplars_from_totals(scan, step, labels).await;
    }
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, {trace}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span}, {trace}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {span}, {trace}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        trace = PCOL_TRACE_ID,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan.collect_sql(&sql).await?;
    let mut per_span: BTreeMap<SpanKey, i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let trace_ids = batch.column(3).as_binary::<i32>() as &BinaryArray;
        let partitions = batch.column(4).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(5).as_primitive::<UInt64Type>();
        let values = batch.column(6).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let frames = scan.symbols.resolve(partition, stacktrace_id);
            if frames_match_call_sites(&frames, call_sites) {
                *per_span
                    .entry(SpanKey {
                        timestamp: timestamps.value(row),
                        fingerprint: fingerprints.value(row),
                        span_id: span_ids.value(row),
                        trace_id: (!trace_ids.is_null(row)).then(|| trace_ids.value(row).to_vec()),
                    })
                    .or_default() += values.value(row);
            }
        }
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for span_entry in per_span {
        let span_key = span_entry.0;
        let value = span_entry.1;
        out.entry(step_bucket_ms(span_key.timestamp, step))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp: span_key.timestamp,
                profile_id: String::new(),
                span_id: span_id_hex_from_u64(span_key.span_id),
                trace_id: span_key.trace_id.map_or_default(hex::encode),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}
