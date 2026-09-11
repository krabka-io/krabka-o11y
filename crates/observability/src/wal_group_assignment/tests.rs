use assert2::{assert, check};

use super::{WalAssignmentChange, WalAssignmentWatch};
use crate::wal_consumer_metrics::WalConsumerMetrics;

fn partition(partition: i32) -> (String, i32) {
    ("wal".to_owned(), partition)
}

#[test]
fn the_first_assignment_is_all_gain_and_no_revocation() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());

    let change = watch.observe(&[partition(0), partition(1)]);

    assert!(
        change
            == WalAssignmentChange {
                revoked: Vec::new(),
                gained: vec![partition(0), partition(1)],
            }
    );
    check!(change.strands_buffered_records() == false);
    check!(metrics.partition_owned("wal", 0) == 1);
    check!(metrics.partition_owned("wal", 1) == 1);
    check!(metrics.partition_revocations("wal", 0) == 0);
    check!(metrics.partition_revocations("wal", 1) == 0);
}

#[test]
fn an_unchanged_assignment_reports_nothing_and_moves_no_counter() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());
    watch.observe(&[partition(0), partition(1)]);

    let change = watch.observe(&[partition(1), partition(0)]);

    check!(change.is_empty());
    check!(metrics.partition_revocations("wal", 0) == 0);
    check!(metrics.partition_revocations("wal", 1) == 0);
    check!(metrics.partition_owned("wal", 0) == 1);
}

#[test]
fn a_partition_that_moves_to_another_member_is_reported_as_revoked() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());
    watch.observe(&[partition(0), partition(1)]);

    let change = watch.observe(&[partition(0)]);

    assert!(
        change
            == WalAssignmentChange {
                revoked: vec![partition(1)],
                gained: Vec::new(),
            }
    );
    check!(change.strands_buffered_records());
    // The partition this member kept is untouched by the revocation of the
    // other one: a rebalance that moves one partition must not report the rest.
    check!(metrics.partition_owned("wal", 0) == 1);
    check!(metrics.partition_revocations("wal", 0) == 0);
    check!(metrics.partition_owned("wal", 1) == 0);
    check!(metrics.partition_revocations("wal", 1) == 1);
}

#[test]
fn an_eager_rebalance_that_replaces_the_whole_assignment_reports_both_halves() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());
    watch.observe(&[partition(0), partition(1)]);

    // The `range` assignor drops the whole assignment and reinstalls it in one
    // round, so a member that keeps working can still see every partition
    // revoked and a different set arrive.
    let change = watch.observe(&[partition(2), partition(3)]);

    assert!(
        change
            == WalAssignmentChange {
                revoked: vec![partition(0), partition(1)],
                gained: vec![partition(2), partition(3)],
            }
    );
    check!(metrics.partition_revocations("wal", 0) == 1);
    check!(metrics.partition_revocations("wal", 1) == 1);
    check!(metrics.partition_owned("wal", 2) == 1);
    check!(metrics.partition_owned("wal", 3) == 1);
}

#[test]
fn losing_every_partition_is_a_revocation_and_not_a_quiet_idle() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());
    watch.observe(&[partition(0)]);

    let change = watch.observe(&[]);

    check!(change.strands_buffered_records());
    check!(metrics.partition_revocations("wal", 0) == 1);
    check!(metrics.partition_owned("wal", 0) == 0);
    check!(watch.owned().is_empty());
}

#[test]
fn each_revocation_of_the_same_partition_counts_again() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());
    watch.observe(&[partition(0)]);
    watch.observe(&[]);
    watch.observe(&[partition(0)]);
    watch.observe(&[]);

    // The gauge returns to 0 either way, so only the counter tells a reader
    // that the partition moved twice.
    check!(metrics.partition_revocations("wal", 0) == 2);
    check!(metrics.partition_owned("wal", 0) == 0);
}

#[test]
fn the_watch_separates_partitions_of_different_topics() {
    let metrics = WalConsumerMetrics::unregistered();
    let mut watch = WalAssignmentWatch::new(metrics.clone());
    watch.observe(&[("wal".to_owned(), 0), ("state".to_owned(), 0)]);

    let change = watch.observe(&[("state".to_owned(), 0)]);

    assert!(
        change
            == WalAssignmentChange {
                revoked: vec![("wal".to_owned(), 0)],
                gained: Vec::new(),
            }
    );
    check!(metrics.partition_revocations("wal", 0) == 1);
    check!(metrics.partition_revocations("state", 0) == 0);
    check!(metrics.partition_owned("state", 0) == 1);
}
