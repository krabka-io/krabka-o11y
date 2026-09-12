use super::{
    DecodedSeries, DistributorState, PushError, TenantId, Time, TimeExt, append_wal_records,
    enforce_ingest_limits, wal_records_from_series,
};

pub(crate) async fn append_decoded_series(
    state: &DistributorState,
    tenant: &TenantId,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    if !enforce_ingest_limits(state, tenant, series, Time::ZERO).await? {
        return Ok(false);
    }
    append_wal_records(
        state,
        tenant,
        wal_records_from_series(tenant.as_str(), series),
    )
    .await?;
    Ok(true)
}
