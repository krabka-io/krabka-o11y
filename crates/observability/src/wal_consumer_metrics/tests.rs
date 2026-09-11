use assert2::check;
use krabka_client_consumer::ConsumerRecord;
use prometheus_client::registry::Registry;

use super::{WalConsumerMetrics, WalPollOutcome};

fn record(topic: &str, partition: i32, offset: i64, timestamp: i64) -> ConsumerRecord {
    ConsumerRecord {
        topic: topic.to_owned(),
        partition,
        offset,
        leader_epoch: 0,
        timestamp,
        key: None,
        value: None,
        headers: Vec::new(),
    }
}

#[test]
fn a_poll_counts_its_records_and_keeps_the_highest_offset_per_partition() {
    let metrics = WalConsumerMetrics::unregistered();
    metrics.record_poll_at(
        &[
            record("wal", 0, 10, 1_000),
            record("wal", 0, 12, 1_000),
            // Out of order within the poll: the highest offset wins, not the
            // last one seen.
            record("wal", 0, 11, 1_000),
            record("wal", 1, 4, 1_000),
        ],
        2_000,
    );

    check!(metrics.records("wal", 0) == 3);
    check!(metrics.records("wal", 1) == 1);
    check!(metrics.last_consumed_offset("wal", 0) == 12);
    check!(metrics.last_consumed_offset("wal", 1) == 4);
    check!(metrics.polls(WalPollOutcome::Records) == 1);
    check!(metrics.polls(WalPollOutcome::Empty) == 0);
}

#[test]
fn an_empty_poll_is_counted_and_moves_no_partition_series() {
    let metrics = WalConsumerMetrics::unregistered();
    metrics.record_poll_at(&[], 2_000);
    metrics.record_poll_at(&[], 3_000);

    check!(
        metrics.polls(WalPollOutcome::Empty) == 2,
        "a caught-up consumer proves it is alive by counting empty polls"
    );
    check!(metrics.polls(WalPollOutcome::Records) == 0);
    check!(metrics.records("wal", 0) == 0);
}

#[test]
fn a_poll_failure_lands_on_its_own_outcome() {
    let metrics = WalConsumerMetrics::unregistered();
    metrics.record_poll_failure();

    check!(metrics.polls(WalPollOutcome::Error) == 1);
    check!(metrics.polls(WalPollOutcome::Empty) == 0);
    check!(
        metrics.polls(WalPollOutcome::Records) == 0,
        "a failed poll is not a poll that returned records"
    );
}

#[test]
fn the_consumed_offset_only_advances_with_the_records_it_was_given() {
    let metrics = WalConsumerMetrics::unregistered();
    metrics.record_poll_at(&[record("wal", 0, 7, 1_000)], 1_000);
    check!(metrics.last_consumed_offset("wal", 0) == 7);

    metrics.record_poll_at(&[record("wal", 0, 9, 1_000)], 1_000);
    check!(metrics.last_consumed_offset("wal", 0) == 9);

    metrics.record_poll_at(&[], 1_000);
    check!(
        metrics.last_consumed_offset("wal", 0) == 9,
        "an empty poll leaves the offset where the last record put it"
    );
}

/// The instruments are read by scraping the registry rather than by reading
/// the handles back, so an instrument that was never registered, or registered
/// under a name no dashboard uses, fails here and only here.
#[test]
fn every_wal_consumer_instrument_reaches_a_scrape_under_the_service_prefix() {
    let mut registry = Registry::with_prefix("krabka_test");
    let metrics = WalConsumerMetrics::register(&mut registry);

    // Produced at 1000 ms, read at 3500 ms: a delay of 2.5 seconds.
    metrics.record_poll_at(&[record("__krabka_test_wal", 3, 41, 1_000)], 3_500);
    metrics.record_poll_at(&[], 3_600);
    metrics.record_poll_failure();

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encoding");

    for needle in [
        "krabka_test_wal_consumer_records_total{topic=\"__krabka_test_wal\",partition=\"3\"} 1",
        "krabka_test_wal_consumer_last_consumed_offset{topic=\"__krabka_test_wal\",partition=\"3\"} 41",
        "krabka_test_wal_consumer_polls_total{outcome=\"records\"} 1",
        "krabka_test_wal_consumer_polls_total{outcome=\"empty\"} 1",
        "krabka_test_wal_consumer_polls_total{outcome=\"error\"} 1",
        "krabka_test_wal_consumer_receive_delay_seconds_count 1",
        "krabka_test_wal_consumer_receive_delay_seconds_sum 2.5",
    ] {
        check!(buffer.contains(needle), "missing {needle} in:\n{buffer}");
    }
}

#[test]
fn a_record_stamped_in_the_future_reports_no_delay_rather_than_a_negative_one() {
    let mut registry = Registry::with_prefix("krabka_test");
    let metrics = WalConsumerMetrics::register(&mut registry);

    // The producer's clock runs ahead of this reader's by a second.
    metrics.record_poll_at(&[record("wal", 0, 1, 5_000)], 4_000);

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encoding");

    check!(
        buffer.contains("krabka_test_wal_consumer_receive_delay_seconds_sum 0.0"),
        "a skewed clock must not make the delay negative:\n{buffer}"
    );
}

/// Kafka spells "no timestamp" as a non-positive one, and a producer that
/// leaves it unset is a producer this workspace has. Subtracting that from the
/// wall clock reports decades, and one such observation ruins every quantile
/// drawn from the histogram, so the record must contribute no observation at
/// all.
#[test]
fn a_record_with_no_produce_timestamp_contributes_no_delay_observation() {
    let mut registry = Registry::with_prefix("krabka_test");
    let metrics = WalConsumerMetrics::register(&mut registry);

    metrics.record_poll_at(&[record("wal", 0, 3, 0)], 1_700_000_000_000);
    metrics.record_poll_at(&[record("wal", 0, 4, -1)], 1_700_000_000_000);

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encoding");

    check!(
        buffer.contains("krabka_test_wal_consumer_receive_delay_seconds_count 0"),
        "an unstamped record must not reach the delay histogram:\n{buffer}"
    );
    check!(
        metrics.records("wal", 0) == 2,
        "the records themselves are still counted"
    );
    check!(
        metrics.last_consumed_offset("wal", 0) == 4,
        "and the offset still advances"
    );
}
