use super::*;

/// Consume WAL records, write span blocks, save the trace index, then commit
/// offsets.
///
/// The loop accumulates decoded windows across polls and merges them per
/// partition, to avoid block proliferation. It flushes a single larger block
/// per partition only once the buffer holds
/// [`BlockBuilderConfig::flush_max_records`] records, or the oldest buffered
/// record reaches [`BlockBuilderConfig::flush_max_age`].
///
/// The loop commits WAL offsets only after it durably writes the merged blocks.
/// It drains the remaining buffer on shutdown, so an orderly stop loses no
/// spans.
///
/// # Consumer group rebalances
///
/// A shutdown is the only assignment change this loop survives. The accumulator
/// holds decoded windows per partition across polls, and the consumer group can
/// take a partition away between two of them. `krabka-client-consumer` releases
/// the partition from a background task and calls nothing in this process
/// first, so the windows buffered for that partition are abandoned.
///
/// [`BlockBuilderConsumer`] reports each such revocation on
/// `wal_consumer_partition_revocations` and in the log. It does not repair it:
/// a later flush still writes a block for a partition this member no longer
/// owns, under a key that is a function of this member's own offset range. See
/// [`krabka_observability::wal_group_assignment`] for why no code here can do
/// better, and for what the group id does and does not do.
///
/// # Object-store failures
///
/// A single 503 or reset connection during a flush used to leave `run`, leave
/// `main`, and end the process. Nothing was lost -- the offsets are committed
/// after the write, never before -- but the role came back cold and re-read
/// the same window, so a fault outlasting a restart became an unbounded retry
/// at *process* granularity. Two bounded retries in place replace it:
///
/// - the trace index's snapshot writes go through a [`RetryingObjectStore`],
///   which retries the backend failures that could clear on their own and
///   reports at once the ones that cannot -- a 403, a 404, a failed
///   precondition; and
/// - `writer` retries each block write as a whole. See [`BlockWriter`] for
///   why the whole write is the only unit a block can be retried in.
///
/// The buffer is not re-taken between attempts: `flush_and_commit` drains the
/// accumulator once and the retries happen underneath it. The block key stays
/// the function of the buffered offset range that [`FlushAccumulator`]
/// promises, so a retried write overwrites its own half-written object instead
/// of leaving a second block beside it, and the commit that follows a
/// successful flush still happens exactly once.
///
/// Both budgets are finite. When one runs out the error propagates and the
/// role exits, because a wrong bucket or a revoked credential must not become
/// a role that is up and silently doing nothing.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn run<C>(
    mut consumer: C,
    writer: BlockWriter,
    index: Arc<Mutex<TraceIndex>>,
    object_store: Arc<dyn ObjectStore>,
    config: BlockBuilderConfig,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), TracesError>
where
    C: WalConsumerPoll + WalConsumerCommit,
{
    let object_store = RetryingObjectStore::wrap(
        object_store,
        ObjectStoreRetryPolicy::DEFAULT,
        metrics.object_store.clone(),
    );
    let mut accumulator = FlushAccumulator::new();
    while !shutdown.is_cancelled() {
        let records = consumer
            .poll(config.window)
            .await
            .inspect_err(|_| metrics.wal_consumer.record_poll_failure())?;
        // Recorded before the decode, so a poll that arrived is counted even
        // when the records in it turn out to be unreadable.
        metrics.wal_consumer.record_poll(&records);
        let windows = decode_consumer_records(&records)?;

        // One consume span per NON-EMPTY poll batch (NOT per record). Parent it
        // to the distributor's ingest span via the W3C trace context carried on
        // any consumed record so the block-build continues the same distributed
        // trace; a no-op when no record carries a `traceparent`. Empty polls run
        // outside the span so age-based flushing is still re-checked without
        // emitting a span per idle round.
        let build_span = (!windows.is_empty()).then(|| {
            let span = tracing::info_span!(
                "traces_block_build",
                otel.kind = "consumer",
                krabka.wal.records = records.len(),
            );
            set_remote_parent_from_records(&span, &records);
            span
        });

        let iteration = async {
            if windows.is_empty() {
                // `poll` normally long-polls for `config.window`, so an empty
                // round already cost a full window. But when every assigned
                // leader hits a transient transport error (e.g. the demo's flaky
                // Docker DNS) `poll` returns `Ok(vec![])` immediately — without
                // this backoff the loop would busy-spin a core. A short sleep
                // bounds that to a trickle.
                tokio::time::sleep(config.empty_poll_backoff.to_std()).await;
            } else {
                accumulator.merge(windows, Instant::now());
            }

            // Flush + commit only when a threshold is reached; a low-traffic
            // stream still flushes within `flush_max_age` because every poll
            // re-checks the age of the oldest buffered record (the empty-poll
            // backoff above bounds the re-check interval). Committing only after
            // `flush_partition_windows` returns `Ok` keeps WAL offsets behind the
            // durable block(s).
            if accumulator.should_flush(&config, Instant::now()) {
                flush_and_commit(
                    &mut consumer,
                    &writer,
                    &index,
                    &object_store,
                    &config,
                    &metrics,
                    &mut accumulator,
                )
                .await?;
            }
            Ok::<(), TracesError>(())
        };

        match build_span {
            Some(span) => iteration.instrument(span).await?,
            None => iteration.await?,
        }
    }

    // Drain the remaining buffer on shutdown so buffered spans are not lost.
    if !accumulator.is_empty() {
        flush_and_commit(
            &mut consumer,
            &writer,
            &index,
            &object_store,
            &config,
            &metrics,
            &mut accumulator,
        )
        .await?;
    }
    Ok(())
}
