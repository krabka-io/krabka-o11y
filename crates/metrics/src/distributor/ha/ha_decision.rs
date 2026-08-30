use super::{DecodedSeries, HaElection, HaTracker, ha_election};

/// Whether a decoded ingest request should append to the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaDecision {
    Accept,
    Drop,
}

/// Synchronous compatibility helper for tests and direct callers.
#[must_use]
pub fn ha_decision(tracker: &HaTracker, tenant: &str, series: &[DecodedSeries]) -> HaDecision {
    match ha_election(tracker, tenant, series) {
        HaElection::Accept => HaDecision::Accept,
        HaElection::Drop => HaDecision::Drop,
        HaElection::Elect(record) | HaElection::Update(record) => {
            tracker.persist_elected(&record);
            HaDecision::Accept
        }
    }
}
