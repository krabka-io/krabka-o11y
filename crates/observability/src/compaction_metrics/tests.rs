use assert2::check;
use krabka_units::{millis, secs};
use prometheus_client::registry::Registry;

use super::CompactionMetrics;

#[test]
fn a_failed_pass_lands_on_the_error_series_and_not_the_ok_one() {
    let metrics = CompactionMetrics::unregistered();
    metrics.record_run(true, secs(1));
    metrics.record_run(false, secs(2));
    metrics.record_run(false, secs(3));

    check!(metrics.runs(true) == 1);
    check!(metrics.runs(false) == 2);
}

#[test]
fn the_block_counter_adds_and_a_zero_changes_nothing() {
    let metrics = CompactionMetrics::unregistered();
    metrics.record_output(2);
    metrics.record_output(3);
    check!(metrics.blocks() == 5);

    metrics.record_output(0);
    check!(
        metrics.blocks() == 5,
        "a pass that wrote nothing adds nothing"
    );
}

/// Scraped through the registry, so a missing registration fails here even
/// though every handle assertion above still passes.
#[test]
fn every_compaction_instrument_reaches_a_scrape_under_the_service_prefix() {
    let mut registry = Registry::with_prefix("krabka_test");
    let metrics = CompactionMetrics::register(&mut registry);

    metrics.record_run(true, millis(250));
    metrics.record_run(false, millis(250));
    metrics.record_output(4);

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encoding");

    for needle in [
        "krabka_test_compaction_runs_total{status=\"ok\"} 1",
        "krabka_test_compaction_runs_total{status=\"error\"} 1",
        "krabka_test_compaction_duration_seconds_count 2",
        "krabka_test_compaction_duration_seconds_sum 0.5",
        "krabka_test_compaction_blocks_total 4",
    ] {
        check!(buffer.contains(needle), "missing {needle} in:\n{buffer}");
    }
}
