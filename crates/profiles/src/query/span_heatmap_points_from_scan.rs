use krabka_pprof::timestamp_total_points;

use super::{COL_FINGERPRINT, COL_TIMESTAMP, PCOL_SPAN_ID, PCOL_VALUE, ProfileError};

pub(crate) async fn span_heatmap_points_from_scan(
    scan: &krabka_pprof::ProfileScan,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, SUM({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_VALUE,
        table = scan.samples_table,
        span = PCOL_SPAN_ID,
        fingerprint = COL_FINGERPRINT,
    );
    timestamp_total_points(scan, &sql).await
}
