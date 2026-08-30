use super::{
    AsArray, BTreeMap, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_TOTAL_VALUE, ProfileError,
    Time, step_bucket_ms,
};

pub(crate) async fn series_buckets_from_totals(
    scan: &crate::ProfileScan,
    step: Time,
) -> Result<BTreeMap<i64, Vec<i64>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint}",
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
    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            buckets
                .entry(step_bucket_ms(timestamps.value(row), step))
                .or_default()
                .push(totals.value(row));
        }
    }
    Ok(buckets)
}
