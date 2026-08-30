use super::{HaTracker, DecodedSeries, Time, HaElection, decide_election};

/// Timestamped HA election helper with an explicit failover timeout.
#[must_use]
/// # Panics
/// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
pub fn ha_election_at_with_timeout(
    tracker: &HaTracker,
    tenant: &str,
    series: &[DecodedSeries],
    lease_timestamp_ms: i64,
    failover_timeout: Time,
) -> HaElection {
    let elected = tracker.elected.lock().expect("HaTracker mutex poisoned");
    decide_election(
        &elected,
        tenant,
        series,
        lease_timestamp_ms,
        failover_timeout,
    )
}
