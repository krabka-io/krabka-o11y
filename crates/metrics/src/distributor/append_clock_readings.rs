use super::{
    DecodedClockReading, DistributorState, PushError, TenantId, UnixNanos, append_wal_records,
    clock_series, clock_wal_records, enforce_ingest_limits, wal_records_from_series,
};

/// Gates a clock batch and appends both the clock block records and the
/// projected float records.
pub(crate) async fn append_clock_readings(
    state: &DistributorState,
    tenant: &TenantId,
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Result<bool, PushError> {
    let mut series = clock_series(readings, ingest_unix_nanos);
    if !enforce_ingest_limits(state, tenant, &mut series).await? {
        return Ok(false);
    }

    let mut records = clock_wal_records(tenant.as_str(), readings, ingest_unix_nanos);
    records.extend(wal_records_from_series(tenant.as_str(), &series));
    append_wal_records(state, tenant, records).await?;
    Ok(true)
}
