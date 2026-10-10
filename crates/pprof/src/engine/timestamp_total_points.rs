use super::{AsArray, Int64Type, ProfileError};

/// Runs `sql` against `scan` and reads its first two `Int64` columns as
/// `(timestamp_ms, total)` points, in result order.
///
/// # Errors
///
/// Returns [`ProfileError::Plan`] when `DataFusion` cannot plan `sql`, and
/// [`ProfileError::Exec`] when executing it fails.
pub async fn timestamp_total_points(
    scan: &crate::ProfileScan,
    sql: &str,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    let batches = scan
        .ctx
        .sql(sql)
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
