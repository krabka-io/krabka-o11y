use super::{
    DecodedSeries, DistributorState, HaElection, PushError, TenantId,
    enforce_and_record_active_series, enforce_creation_grace_period, enforce_ingestion_rate,
    enforce_label_limits, enforce_out_of_order_window, strip_replica_label, validate,
};

/// Applies every per-tenant ingest gate to `series`, in the order the push path
/// needs them.
///
/// The tenant's limits are resolved once here and handed to every gate, so one
/// request cannot get two verdicts on the same limit.
///
/// Returns `false` when the HA tracker drops the request, which is an accepted
/// request that writes nothing.
pub(crate) async fn enforce_ingest_limits(
    state: &DistributorState,
    tenant: &TenantId,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    let limits = state.limits_for_tenant(tenant);
    let now = state.clock.now();
    validate(series, limits)?;
    enforce_label_limits(limits, series)?;
    enforce_creation_grace_period(limits, series, state.clock.now_unix_ms())?;
    // Decide-and-commit the in-memory HA winner atomically so a racing replica
    // cannot also win the same (tenant, cluster); only the durable Kafka persist
    // is left async, after the in-memory winner is already fixed.
    match state
        .tracker
        .elect_now_with_timeout(tenant.as_str(), series, state.ha_failover_timeout)
    {
        HaElection::Accept => {}
        HaElection::Drop => return Ok(false),
        HaElection::Elect(record) | HaElection::Update(record) => {
            // The in-memory winner is already committed under the tracker lock;
            // only the durable Kafka persist remains and may proceed async.
            if let Some(sink) = &state.ha_election_sink {
                sink.persist_election(record.clone()).await?;
            }
        }
    }

    strip_replica_label(series);
    enforce_and_record_active_series(state, limits, tenant, series, now)?;
    enforce_ingestion_rate(state, limits, tenant, series)?;
    enforce_out_of_order_window(state, limits, tenant, series, now)?;
    Ok(true)
}
