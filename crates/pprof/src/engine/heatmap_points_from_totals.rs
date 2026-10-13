use super::{
    COL_FINGERPRINT, COL_TIMESTAMP, PCOL_TOTAL_VALUE, ProfileError, TimestampTotalPoint,
    timestamp_total_points,
};

pub(crate) async fn heatmap_points_from_totals(
    scan: &crate::ProfileScan,
) -> Result<Vec<TimestampTotalPoint>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        fingerprint = COL_FINGERPRINT,
    );
    timestamp_total_points(scan, &sql).await
}
