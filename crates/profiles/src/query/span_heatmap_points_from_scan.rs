use super::*;

pub(crate) async fn span_heatmap_points_from_scan(
    scan: &krabka_pprof::ProfileScan,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        span = PCOL_SPAN_ID,
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
    let mut points = Vec::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            points.push((timestamps.value(row), totals.value(row)));
        }
    }
    Ok(points)
}
