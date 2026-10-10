//! Counting a compactor role's passes, for the supervision suites of the
//! signal binaries that run one.
//!
//! The run counter is the only thing outside a compactor role that moves once
//! per pass, so it is also how a loop that outlived its role is caught. The
//! `krabka-traces` and `krabka-profiles` binaries reach this file with
//! `#[path]`.

use std::time::{Duration, Instant};

use krabka_observability::compaction_metrics::CompactionMetrics;

/// The compaction interval the suites run the role at: short enough that
/// several passes happen while a test waits, and still long enough to be a
/// schedule rather than a spin.
pub const COMPACTION_INTERVAL: &str = "20ms";

/// Long enough for ten ticks of [`COMPACTION_INTERVAL`], so a loop that was
/// left running has recorded passes by the time the check reads the counter.
const AFTER_THE_STOP: Duration = Duration::from_millis(200);

/// Passes the role has finished, whatever each one made of the store.
pub fn passes(compaction: &CompactionMetrics) -> u64 {
    compaction.runs(true) + compaction.runs(false)
}

/// Waits until the role has finished `wanted` passes, so a test acts on a
/// compactor that is demonstrably running rather than on a guess at a delay.
pub async fn passes_reach(compaction: &CompactionMetrics, wanted: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if passes(compaction) >= wanted {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "the compactor finished {} passes, and the test waited for {wanted}",
        passes(compaction)
    );
}

/// Asserts that the role finishes no further pass during [`AFTER_THE_STOP`],
/// starting now.
///
/// A test calls this once its role or stage has returned. A loop that
/// outlived its owner would still be ticking and raising the run counter, and
/// the test then fails with `outlived_message`.
pub async fn assert_no_pass_after_the_stop(compaction: &CompactionMetrics, outlived_message: &str) {
    let stopped_at = passes(compaction);
    tokio::time::sleep(AFTER_THE_STOP).await;
    assert2::assert!(passes(compaction) == stopped_at, "{outlived_message}");
}
