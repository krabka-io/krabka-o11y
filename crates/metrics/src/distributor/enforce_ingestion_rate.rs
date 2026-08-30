use super::{DistributorState, Limits, DecodedSeries, PushError, decoded_sample_count};

pub(crate) fn enforce_ingestion_rate(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    let sample_count = decoded_sample_count(series);
    if sample_count == 0 {
        return Ok(());
    }

    state
        .ingest_enforcer
        .check_sample_rate(
            limits,
            tenant,
            u64::try_from(sample_count).unwrap_or(u64::MAX),
        )
        .map_err(PushError::from)
}
