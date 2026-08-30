use super::{HaElectionRecord, HaTracker, DecodedSeries, ha_election_at, now_ms};

/// The HA election action required for a decoded ingest request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaElection {
    Accept,
    Drop,
    Elect(HaElectionRecord),
    Update(HaElectionRecord),
}

/// Inspects the `cluster` and `__replica__` labels of the first series. A
/// missing `__replica__` means HA is off for the request, and the distributor
/// accepts the request. Otherwise only the elected replica may write.
#[must_use]
pub fn ha_election(tracker: &HaTracker, tenant: &str, series: &[DecodedSeries]) -> HaElection {
    ha_election_at(tracker, tenant, series, now_ms())
}
