use super::{AsArray, Int64Type, ProfileError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampTotalPoint {
    pub timestamp_ms: i64,
    pub total: i64,
}

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
) -> Result<Vec<TimestampTotalPoint>, ProfileError> {
    let batches = scan.collect_sql(sql).await?;
    let mut points = Vec::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            points.push(TimestampTotalPoint {
                timestamp_ms: timestamps.value(row),
                total: totals.value(row),
            });
        }
    }
    Ok(points)
}
