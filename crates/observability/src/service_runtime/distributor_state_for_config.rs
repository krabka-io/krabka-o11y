use super::{
    AllowAllIngestLimiter, Arc, DistributorState, ServiceConfig, ServiceConfigError,
    ServiceDependencies, ServiceMetrics,
};
use crate::ReadinessGate;

/// The distributor's state, holding the gate a drain clears.
///
/// The gate is taken rather than registered here so that exactly one exists
/// per distributor. An all-in-one both serves `POST
/// /ingester/prepare_shutdown` from this state and clears the same gate as the
/// first step of its stop; registering one gate in each place would leave
/// `/ready` listing `distributor/accepting-writes` twice and an operator's
/// drain clearing only one of them, so the probe would go on reporting a
/// distributor that is willing to take writes it no longer wants.
///
/// # Errors
/// Returns [`ServiceConfigError::MissingWalSink`] when no WAL sink was built.
pub(crate) fn distributor_state_for_config(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
    metrics: ServiceMetrics,
    accepting_writes: ReadinessGate,
) -> Result<DistributorState, ServiceConfigError> {
    let sink = dependencies
        .wal_sink
        .clone()
        .ok_or(ServiceConfigError::MissingWalSink)?;
    let ingest_limiter = dependencies
        .ingest_limiter
        .clone()
        .unwrap_or_else(|| Arc::new(AllowAllIngestLimiter));
    // Met from the start: a distributor that has its WAL sink can take writes.
    // An operator's drain request, or the first step of an all-in-one's stop,
    // is what drops it.
    accepting_writes.mark_ready();
    Ok(DistributorState {
        sink,
        ingest_limiter,
        prepare_shutdown: accepting_writes,
        max_ingest_body: config.max_ingest_body,
        wal_append_timeout: config.wal_append_timeout,
        reject_old_samples_max_age: Some(config.reject_old_samples_max_age),
        creation_grace_period: Some(config.creation_grace_period),
        metrics,
    })
}
