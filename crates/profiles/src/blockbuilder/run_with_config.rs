use super::*;

/// Builds profile blocks from the WAL until `shutdown` is cancelled.
///
/// The cancellation is a drain, not an abort. Whatever the accumulator holds
/// when the signal arrives is flushed into a block, the index snapshot is
/// saved, and the consumer's offset is committed -- in that order -- before
/// the loop returns. Returning any earlier would abandon a partially written
/// block and leave the offset behind it uncommitted, so the next start would
/// replay that window: work silently lost on an ordinary rolling restart.
///
/// # Object-store failures
///
/// An object store that fails in a way that could clear on its own -- a 503, a
/// timeout, a reset connection -- no longer ends the role. The profile index's
/// snapshot reads and writes go through a [`RetryingObjectStore`] carrying
/// [`BlockBuilderConfig::object_store_retry`], and each block write is retried
/// as a whole by [`BlockWriter`](krabka_blockstore::BlockWriter). A failure
/// that says the request itself is wrong -- a 403, a 404, a failed
/// precondition -- is reported on the first attempt instead, and so is one
/// that outlasts the budget. The role still exits in that case, which is the
/// point: a wrong bucket must not turn into a role that is up and doing
/// nothing.
///
/// The flush is retried underneath `accumulator.take()`, never around it, so
/// the records are not re-drained and each block keeps the key its WAL offset
/// range gives it. A retry overwrites its own object rather than adding one.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run_with_config(
    config: BlockBuilderConfig,
    shutdown: CancellationToken,
) -> Result<(), ProfilesError> {
    // The index store is a separate handle on purpose. `config.store` stays
    // unwrapped because `build_block` hands it to a `BlockWriter`, which does
    // its own whole-write retry; wrapping it as well would multiply the two
    // budgets together.
    // An unwired bundle records nothing, and that is the right reading for a
    // builder a test drives with no registry behind it.
    let object_store_metrics = config
        .metrics
        .as_ref()
        .map_or_else(ObjectStoreMetrics::unregistered, |metrics| {
            metrics.object_store.clone()
        });
    let index_store = RetryingObjectStore::wrap(
        Arc::clone(&config.store),
        config.object_store_retry,
        object_store_metrics.clone(),
    );
    let mut index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &index_store,
        &config.index_key,
        config.index_snapshot_max,
    )
    .await
    .map_err(|error| ProfilesError::Block(format!("profile index load failed: {error}")))?;
    let mut consumer = Consumer::builder()
        .bootstrap(config.bootstrap)
        .dispatch_queue_capacity(config.client_dispatch_queue_capacity.get())
        .frame_max(config.client_frame_max.size())
        .group_id(config.group_id.clone())
        .fetch_max(config.wal_fetch_max)
        .fetch_partition_max(config.wal_fetch_partition_max)
        .subscribe(vec![config.wal_topic.clone()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .map_err(|err| ProfilesError::Block(format!("consumer build failed: {err}")))?;

    let mut accumulator =
        ConsumerRecordAccumulator::new(config.flush_records, config.flush_max_age);
    loop {
        let records = tokio::select! {
            biased;
            () = shutdown.cancelled() => Vec::new(),
            polled = consumer.poll(config.poll_timeout) => {
                let polled = polled.inspect_err(|_| {
                    if let Some(metrics) = &config.metrics {
                        metrics.wal_consumer.record_poll_failure();
                    }
                });
                let polled = polled
                    .map_err(|err| ProfilesError::Block(format!("consumer poll failed: {err}")))?;
                if let Some(metrics) = &config.metrics {
                    metrics.wal_consumer.record_poll(&polled);
                }
                polled
            }
        };
        let draining = shutdown.is_cancelled();
        let now = Instant::now();
        accumulator.push(records, now);
        if !draining && !accumulator.should_flush(now) {
            continue;
        }
        let records = accumulator.take();
        if records.is_empty() {
            if draining {
                return Ok(());
            }
            continue;
        }
        // ONE consumer span per poll batch (not per record). Re-parent it onto
        // the ingest span of a record carrying `traceparent`, stitching the
        // block-build stage onto the distributed trace that produced the WAL.
        let build_span = tracing::info_span!(
            "profiles_block_build",
            otel.kind = "consumer",
            krabka.wal.records = records.len(),
        );
        if let Some(rec) = records
            .iter()
            .find(|rec| rec.headers.iter().any(|h| h.key == "traceparent"))
        {
            krabka_telemetry::propagation::set_remote_parent(
                &build_span,
                rec.headers
                    .iter()
                    .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
            );
        }
        async {
            let metas = flush_consumer_records_with_index(
                &config.store,
                &mut index,
                &records,
                config.flush_records,
                &object_store_metrics,
            )
            .await?;
            if let Some(metrics) = &config.metrics {
                metrics.record_blocks_built(metas.len() as u64);
            }
            index
                .save_latest_snapshot_with_retain(
                    &index_store,
                    &config.index_key,
                    config.index_snapshot_retain,
                )
                .await
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
            consumer
                .commit_sync()
                .await
                .map_err(|err| ProfilesError::Block(format!("consumer commit failed: {err}")))?;
            Ok::<(), ProfilesError>(())
        }
        .instrument(build_span)
        .await?;
        if draining {
            return Ok(());
        }
    }
}
