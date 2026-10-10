use super::{BTreeMap, ProfileError, Time, call_site_profile_totals, step_bucket_ms};

pub(crate) async fn series_buckets_from_stacktrace_selector(
    scan: &crate::ProfileScan,
    step: Time,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<i64>>, ProfileError> {
    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for (timestamp, value) in call_site_profile_totals(scan, call_sites).await? {
        buckets
            .entry(step_bucket_ms(timestamp, step))
            .or_default()
            .push(value);
    }
    Ok(buckets)
}
