use super::{BTreeMap, ProfileError, Time, heatmap_points_from_totals, step_bucket_ms};

pub(crate) async fn series_buckets_from_totals(
    scan: &crate::ProfileScan,
    step: Time,
) -> Result<BTreeMap<i64, Vec<i64>>, ProfileError> {
    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for (timestamp, total) in heatmap_points_from_totals(scan).await? {
        buckets
            .entry(step_bucket_ms(timestamp, step))
            .or_default()
            .push(total);
    }
    Ok(buckets)
}
