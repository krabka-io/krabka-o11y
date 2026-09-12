use krabka_metrics::DeferredBlockDeletions;
use krabka_observability::CancellationToken;
use krabka_units::Time;

use super::{
    Arc, BlockWriter, CompactionPolicy, ObjectStore, ObjectStoreCompactionIndexSink,
    ServiceMetrics, TimeExt, run_compactor_once,
};

/// Runs compaction passes on `--compactor-interval` until `shutdown` fires.
///
/// Everything a pass needs is owned rather than borrowed, because the role
/// supervises this loop rather than awaiting it inline: a supervised task holds
/// its own state for as long as it runs. See
/// [`run_compactor`](super::run_compactor::run_compactor) for what supervision
/// buys.
///
/// The tick skips a missed deadline rather than firing twice. A pass that ran
/// long has already read whatever the skipped tick would have read.
///
/// The queue of retired input blocks lives here, across ticks, because the
/// blocks a pass retires are deleted by a *later* pass. See
/// [`DeferredBlockDeletions`].
// cargo-mutants: the wall-clock schedule is exercised through the pass itself.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn compactor_loop(
    store: Arc<dyn ObjectStore>,
    block_writer: BlockWriter,
    index_sink: ObjectStoreCompactionIndexSink,
    policy: CompactionPolicy,
    interval: Time,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) {
    let mut deferred = DeferredBlockDeletions::new();
    let mut tick = tokio::time::interval(interval.to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        let started = std::time::Instant::now();
        let outcome = run_compactor_once(
            &store,
            &block_writer,
            &index_sink,
            policy,
            &mut deferred,
            &metrics,
        )
        .await;
        metrics
            .compaction
            .record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
        match outcome {
            Ok(pass) => {
                for failure in pass
                    .manifests_retired
                    .failures
                    .iter()
                    .chain(pass.blocks_deleted.failures.iter())
                {
                    tracing::warn!(
                        object = %failure.failed_key,
                        error = %failure.error,
                        "metrics compactor could not delete a retired object"
                    );
                }
                if !pass.is_empty() {
                    tracing::info!(
                        compacted_blocks = pass.outputs.len(),
                        manifests_retired = pass.manifests_retired.deleted,
                        blocks_deleted = pass.blocks_deleted.deleted,
                        deletions_deferred = deferred.keys().len(),
                        "metrics compactor finished one pass"
                    );
                }
            }
            // One failed pass is not a reason to lose the role. The next tick
            // reloads the manifests and replans from whatever is durable. The
            // counter is what makes that visible: without it a role whose every
            // pass fails exports what a role with nothing to do exports.
            Err(error) => {
                tracing::warn!(
                    %error,
                    "metrics compaction pass failed; retrying on the next tick"
                );
            }
        }
    }
}
