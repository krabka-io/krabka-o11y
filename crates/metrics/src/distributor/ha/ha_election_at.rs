use super::{DEFAULT_HA_FAILOVER_TIMEOUT, DecodedSeries, HaElection, HaTracker, ha_election_at_with_timeout};

/// Timestamped HA election helper for deterministic tests and injectable clocks.
#[must_use]
pub fn ha_election_at(
    tracker: &HaTracker,
    tenant: &str,
    series: &[DecodedSeries],
    lease_timestamp_ms: i64,
) -> HaElection {
    ha_election_at_with_timeout(
        tracker,
        tenant,
        series,
        lease_timestamp_ms,
        DEFAULT_HA_FAILOVER_TIMEOUT,
    )
}
