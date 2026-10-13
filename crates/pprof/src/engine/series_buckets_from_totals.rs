use super::{
    BTreeMap, ProfileError, SeriesBucket, Time, heatmap_points_from_totals, into_series_buckets,
    step_bucket_ms,
};

pub(crate) async fn series_buckets_from_totals(
    scan: &crate::ProfileScan,
    step: Time,
) -> Result<Vec<SeriesBucket>, ProfileError> {
    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for point in heatmap_points_from_totals(scan).await? {
        buckets
            .entry(step_bucket_ms(point.timestamp_ms, step))
            .or_default()
            .push(point.total);
    }
    Ok(into_series_buckets(buckets))
}
