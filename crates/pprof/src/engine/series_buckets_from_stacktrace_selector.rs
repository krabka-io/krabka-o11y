use super::{
    AsArray, BTreeMap, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_VALUE, ProfileError, Time, UInt64Type,
    stack_matches_call_sites, step_bucket_ms,
};

pub(crate) async fn series_buckets_from_stacktrace_selector(
    scan: &crate::ProfileScan,
    step: Time,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<i64>>, ProfileError> {
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
            if stack_matches_call_sites(&frames, call_sites) {
                *per_profile
                    .entry((timestamps.value(row), fingerprints.value(row)))
                    .or_default() += values.value(row);
            }
        }
    }

    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for ((timestamp, _fingerprint), value) in per_profile {
        buckets
            .entry(step_bucket_ms(timestamp, step))
            .or_default()
            .push(value);
    }
    Ok(buckets)
}
