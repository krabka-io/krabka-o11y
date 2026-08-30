use super::{DecodedSeries, DistributorState, PushError, append_wal_records, enforce_ingest_limits, wal_records_from_series};

pub(crate) async fn append_decoded_series(
    state: &DistributorState,
    tenant: &str,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    if !enforce_ingest_limits(state, tenant, series).await? {
        return Ok(false);
    }
    append_wal_records(state, tenant, wal_records_from_series(tenant, series)).await?;
    Ok(true)
}
