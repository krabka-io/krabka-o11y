use super::*;

pub(crate) fn ingestion_bucket_for_tenant(
    state: &DistributorState,
    tenant: &str,
    rate: Frequency,
) -> Result<Arc<TokenBucket>, ProfilesError> {
    let mut buckets = state
        .ingestion_buckets
        .lock()
        .map_err(|_| ProfilesError::Internal("ingestion bucket lock poisoned".to_string()))?;
    // Bound per-tenant map growth: evict an arbitrary
    // existing tenant before admitting a brand-new one once the cap is hit.
    if !buckets.contains_key(tenant) && buckets.len() >= state.max_tracked_tenants {
        evict_one_tenant(&mut buckets);
    }
    let bucket = buckets
        .entry(tenant.to_string())
        .or_insert_with(|| Arc::new(TokenBucket::new()))
        .clone();
    // One token is one profile sample, not one byte.
    if bucket.event_rate() != rate {
        bucket.set_event_rate(rate);
    }
    Ok(bucket)
}
