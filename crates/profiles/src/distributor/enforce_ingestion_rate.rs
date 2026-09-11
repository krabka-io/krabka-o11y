use super::*;

/// Gate a batch against the tenant's ingestion rate and burst.
///
/// The limits come from the one overrides provider, so a tenant with no entry
/// of its own is gated by the file's defaults. A zero `ingestion_rate` is
/// unlimited, as in Pyroscope, and turns the gate off for that tenant.
pub(crate) fn enforce_ingestion_rate(
    state: &DistributorState,
    tenant: &TenantId,
    profile_count: usize,
) -> Result<(), ProfilesError> {
    if profile_count == 0 {
        return Ok(());
    }
    let limits = state.overrides.for_tenant(tenant);
    if limits.ingestion_rate.per_sec_f64() <= 0.0 {
        return Ok(());
    }
    let requested = u64::try_from(profile_count).unwrap_or(u64::MAX);
    if limits.ingestion_burst_profiles > 0 && requested > limits.ingestion_burst_profiles {
        return Err(crate::limits::LimitError::IngestionRateExceeded {
            rate: limits.ingestion_rate.per_sec_f64(),
            observed: requested.to_f64().unwrap_or(f64::MAX),
        }
        .into());
    }

    let configured_rate = rate_tokens_per_sec(limits);
    let bucket = ingestion_bucket_for_tenant(
        state,
        tenant.as_str(),
        Frequency::from_per_sec_u64(configured_rate),
    )?;
    let granted = bucket.try_consume(requested);
    if granted < requested {
        return Err(crate::limits::LimitError::IngestionRateExceeded {
            rate: limits.ingestion_rate.per_sec_f64(),
            observed: requested.to_f64().unwrap_or(f64::MAX),
        }
        .into());
    }
    Ok(())
}
