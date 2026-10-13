use super::{
    BTreeMap, ProfileError, SeriesBucket, Time, call_site_profile_totals, into_series_buckets,
    step_bucket_ms,
};

pub(crate) async fn series_buckets_from_stacktrace_selector(
    scan: &crate::ProfileScan,
    step: Time,
    call_sites: &[String],
) -> Result<Vec<SeriesBucket>, ProfileError> {
    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for point in call_site_profile_totals(scan, call_sites).await? {
        buckets
            .entry(step_bucket_ms(point.timestamp_ms, step))
            .or_default()
            .push(point.total);
    }
    Ok(into_series_buckets(buckets))
}
