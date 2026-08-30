//! HA deduplication for Prometheus replica pairs.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use krabka_units::prelude::*;

use crate::wire::DecodedSeries;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::Labels;

    use super::*;
    use crate::wire::DecodedSample;

    fn series_with(cluster: &str, replica: &str) -> DecodedSeries {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("cluster", cluster);
        labels.insert("__replica__", replica);
        DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(1, 1.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }
    }

    #[test]
    fn elected_replica_accepts() {
        let tracker = HaTracker::default();
        tracker.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r1")];

        assert!(ha_decision(&tracker, "tenant", &series) == HaDecision::Accept);
    }

    #[test]
    fn non_elected_replica_drops() {
        let tracker = HaTracker::default();
        tracker.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r2")];

        assert!(ha_decision(&tracker, "tenant", &series) == HaDecision::Drop);
    }

    #[test]
    fn first_seen_replica_elected_second_dropped() {
        let tracker = HaTracker::default();
        let r1 = [series_with("c1", "r1")];
        let r2 = [series_with("c1", "r2")];

        check!(ha_decision(&tracker, "tenant", &r1) == HaDecision::Accept);
        check!(ha_decision(&tracker, "tenant", &r2) == HaDecision::Drop);
        check!(tracker.elected_replica("tenant", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn elected_replica_updates_lease_timestamp() {
        let tracker = HaTracker::default();
        tracker.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r1")];

        assert!(
            ha_election_at(&tracker, "tenant", &series, 42_000)
                == HaElection::Update(HaElectionRecord {
                    tenant: "tenant".to_string(),
                    cluster: "c1".to_string(),
                    replica: "r1".to_string(),
                    lease_timestamp_ms: 42_000,
                })
        );
    }

    #[test]
    fn stale_elected_replica_can_fail_over() {
        let tracker = HaTracker::default();
        tracker.persist_elected(&HaElectionRecord {
            tenant: "tenant".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 10_000,
        });
        let replacement = [series_with("c1", "r2")];

        assert!(
            ha_election_at_with_timeout(&tracker, "tenant", &replacement, 45_001, secs(30))
                == HaElection::Elect(HaElectionRecord {
                    tenant: "tenant".to_string(),
                    cluster: "c1".to_string(),
                    replica: "r2".to_string(),
                    lease_timestamp_ms: 45_001,
                })
        );
    }

    #[test]
    fn negative_failover_timeout_disables_takeover() {
        // A negative extent is the "never fail over" sentinel: however stale the
        // lease, the incumbent keeps it and the challenger is dropped.
        let tracker = HaTracker::default();
        tracker.persist_elected(&HaElectionRecord {
            tenant: "tenant".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 10_000,
        });
        let replacement = [series_with("c1", "r2")];

        assert!(
            ha_election_at_with_timeout(
                &tracker,
                "tenant",
                &replacement,
                i64::MAX,
                Time::from_millis(-1),
            ) == HaElection::Drop
        );
    }

    #[test]
    fn configured_failover_timeout_controls_takeover() {
        let tracker = || {
            let tracker = HaTracker::default();
            tracker.persist_elected(&HaElectionRecord {
                tenant: "tenant".to_owned(),
                cluster: "c1".to_owned(),
                replica: "r1".to_owned(),
                lease_timestamp_ms: 1_000,
            });
            tracker
        };
        let replacement = [series_with("c1", "r2")];

        check!(
            tracker().elect("tenant", &replacement, i64::MAX, Time::from_millis(-1_000),)
                == HaElection::Drop
        );
        check!(matches!(
            tracker().elect("tenant", &replacement, 1_001, Time::ZERO),
            HaElection::Elect(_)
        ));
        check!(
            tracker().elect("tenant", &replacement, 2_000, millis(999))
                == HaElection::Elect(HaElectionRecord {
                    tenant: "tenant".to_owned(),
                    cluster: "c1".to_owned(),
                    replica: "r2".to_owned(),
                    lease_timestamp_ms: 2_000,
                })
        );

        // The takeover needs the lease to be stale by MORE than the timeout,
        // not merely as stale as it. One millisecond either side of exactly
        // 1_000ms elapsed is the only pair that separates `>` from `>=`, and
        // getting it wrong hands the cluster to a second replica a whole
        // interval early -- both then write, which is what HA exists to stop.
        check!(
            tracker().elect("tenant", &replacement, 2_000, millis(1_000)) == HaElection::Drop,
            "exactly at the timeout is not yet stale"
        );
        check!(matches!(
            tracker().elect("tenant", &replacement, 2_001, millis(1_000)),
            HaElection::Elect(_)
        ));
    }

    #[test]
    fn no_replica_label_means_ha_disabled() {
        let tracker = HaTracker::default();
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        let series = [DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(1, 1.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }];

        assert!(ha_decision(&tracker, "tenant", &series) == HaDecision::Accept);
    }

    #[test]
    fn strip_removes_replica_label() {
        let mut series = vec![series_with("c1", "r1")];

        strip_replica_label(&mut series);

        assert!(series[0].labels.get("__replica__") == None);
        assert!(series[0].labels.get("cluster") == Some("c1"));
    }

    #[test]
    fn elect_commits_in_memory_winner_atomically() {
        let tracker = HaTracker::default();
        let r1 = [series_with("c1", "r1")];
        let r2 = [series_with("c1", "r2")];

        assert!(matches!(
            tracker.elect("tenant", &r1, 1_000, DEFAULT_HA_FAILOVER_TIMEOUT),
            HaElection::Elect(_)
        ));
        // The first elect already committed the winner under the lock, so a
        // competing replica observes it and is dropped without a separate
        // persist step.
        assert!(
            tracker.elect("tenant", &r2, 1_001, DEFAULT_HA_FAILOVER_TIMEOUT) == HaElection::Drop
        );
        assert!(tracker.elected_replica("tenant", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn concurrent_first_seen_elections_elect_exactly_one() {
        use std::{
            sync::{Arc, Barrier},
            thread,
        };

        let tracker = Arc::new(HaTracker::default());
        let barrier = Arc::new(Barrier::new(2));

        let handles = ["ra", "rb"].map(|replica| {
            let tracker = Arc::clone(&tracker);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let series = [series_with("c1", replica)];
                barrier.wait();
                tracker.elect("tenant", &series, 1_000, DEFAULT_HA_FAILOVER_TIMEOUT)
            })
        });

        let elects = handles
            .into_iter()
            .map(|handle| handle.join().expect("election thread panicked"))
            .filter(|decision| matches!(decision, HaElection::Elect(_)))
            .count();

        assert!(elects == 1);
    }
}

// === split-modules: generated submodules ===
mod decide_election;
mod default_ha_failover_timeout;
mod ha_decision;
mod ha_election;
mod ha_election_at;
mod ha_election_at_with_timeout;
mod ha_election_record;
mod ha_election_record_error;
mod ha_tracker;
mod ha_tracker_topic;
mod now_ms;
mod strip_replica_label;

use decide_election::decide_election;
pub use default_ha_failover_timeout::DEFAULT_HA_FAILOVER_TIMEOUT;
pub use ha_decision::{HaDecision, ha_decision};
pub use ha_election::{HaElection, ha_election};
pub use ha_election_at::ha_election_at;
pub use ha_election_at_with_timeout::ha_election_at_with_timeout;
pub use ha_election_record::HaElectionRecord;
pub use ha_election_record_error::HaElectionRecordError;
pub use ha_tracker::HaTracker;
pub use ha_tracker_topic::HA_TRACKER_TOPIC;
use now_ms::now_ms;
pub use strip_replica_label::strip_replica_label;
