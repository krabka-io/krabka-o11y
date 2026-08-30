use super::{DistributorState, DecodedSeries, PushError, validate, enforce_label_limits, HaElection, strip_replica_label, enforce_and_record_active_series, enforce_ingestion_rate, enforce_out_of_order_window};

/// Applies every per-tenant ingest gate to `series`, in the order the push path
/// needs them.
///
/// Returns `false` when the HA tracker drops the request, which is an accepted
/// request that writes nothing.
pub(crate) async fn enforce_ingest_limits(
    state: &DistributorState,
    tenant: &str,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    validate(series, &state.limits)?;
    let limits = state.limits_for_tenant(tenant);
    enforce_label_limits(&limits, series)?;
    // Decide-and-commit the in-memory HA winner atomically so a racing replica
    // cannot also win the same (tenant, cluster); only the durable Kafka persist
    // is left async, after the in-memory winner is already fixed.
    match state
        .tracker
        .elect_now_with_timeout(tenant, series, state.ha_failover_timeout)
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
    enforce_and_record_active_series(state, &limits, tenant, series)?;
    enforce_ingestion_rate(state, &limits, tenant, series)?;
    enforce_out_of_order_window(state, &limits, tenant, series)?;
    Ok(true)
}
