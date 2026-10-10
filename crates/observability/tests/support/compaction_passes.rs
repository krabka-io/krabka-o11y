//! Counting a compactor role's passes, for the supervision suites of the
//! signal binaries that run one.
//!
//! The run counter is the only thing outside a compactor role that moves once
//! per pass, so it is also how a loop that outlived its role is caught. The
//! `krabka-traces` and `krabka-profiles` binaries reach this file with
//! `#[path]`.

use std::time::{Duration, Instant};

use krabka_observability::{CancellationToken, StagedDrain, compaction_metrics::CompactionMetrics};
use krabka_units::secs;

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

/// Asserts that a role started with its token already cancelled returned
/// `Ok(())` without finishing a pass.
///
/// That is the shutdown arriving during startup: the stop is not a fault, and
/// nothing was compacted on the way out.
pub fn assert_stopped_without_a_pass<RoleError: std::fmt::Debug>(
    outcome: &Result<(), RoleError>,
    compaction: &CompactionMetrics,
) {
    assert2::check!(outcome.is_ok(), "{outcome:?}");
    assert2::check!(passes(compaction) == 0);
}

/// Runs `compactor_stage` as the one stage of a [`StagedDrain`], drains it
/// once it has finished two passes, and asserts that it stopped inside the
/// drain's budget and took its loop with it.
///
/// This is the `--target all` stop: a stage that returned while its loop kept
/// running, or that did not return at all, would hold the whole stop open or
/// outlive it. The caller checks how the stage itself ended.
pub async fn assert_stage_drains_with_its_loop<Stage, StageRun>(
    compaction: &CompactionMetrics,
    compactor_stage: Stage,
) where
    Stage: FnOnce(CancellationToken) -> StageRun,
    StageRun: Future<Output = ()> + Send + 'static,
{
    let mut drain = StagedDrain::new(secs(5));
    drain.stage("compactor", compactor_stage);

    passes_reach(compaction, 2).await;
    let overran = drain.drain().await;

    assert2::assert!(overran.is_empty(), "the compactor stage overran its budget");
    assert_no_pass_after_the_stop(
        compaction,
        "a compaction loop outlived the stage that owned it",
    )
    .await;
}
