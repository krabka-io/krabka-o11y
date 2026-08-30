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
/// It drains the remaining buffer on shutdown, so no spans are lost.
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
    let mut accumulator = FlushAccumulator::new();
    while !shutdown.is_cancelled() {
        let records = consumer.poll(config.window).await?;
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
