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
/// # Consumer group rebalances
///
/// The drain covers a shutdown this loop is told about. It does not cover a
/// consumer group that takes a partition away. The accumulator holds records
/// across polls, and `krabka-client-consumer` releases a partition from a
/// background task without calling anything in this process, so the records
/// buffered for that partition are abandoned.
///
/// The loop reports each such revocation on
/// `wal_consumer_partition_revocations` and in the log. It does not repair it.
/// See [`krabka_observability::wal_group_assignment`] for why, and for what the
/// group id does and does not do.
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
    // Both of these are raced against the token rather than awaited. An object
    // store or a broker that has gone away makes its connect take as long as
    // its own retry budget, and a `SIGTERM` that arrives during the start has
    // nothing to cancel otherwise: the role would run to the end of the
    // orchestrator's grace period and be `SIGKILL`ed. Neither has buffered a
    // record yet, so returning here drains nothing.
    let mut index = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(()),
        loaded = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
            &index_store,
            &config.index_key,
            config.index_snapshot_max,
        ) => loaded.map_err(|error| {
            ProfilesError::Block(format!("profile index load failed: {error}"))
        })?,
    };
    let mut consumer = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(()),
        built = Consumer::builder()
            .bootstrap(config.bootstrap)
            .dispatch_queue_capacity(config.client_dispatch_queue_capacity.get())
            .frame_max(config.client_frame_max.size())
            .group_id(config.group_id.clone())
            .fetch_max(config.wal_fetch_max)
            .fetch_partition_max(config.wal_fetch_partition_max)
            .subscribe(vec![config.wal_topic.clone()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build() => built.map_err(|err| {
                ProfilesError::Block(format!("consumer build failed: {err}"))
            })?,
    };

    // Reports a group rebalance that takes WAL partitions away from this
    // member. The builder buffers records across polls, so a revocation
    // abandons whatever it holds for the lost partitions, and nothing in this
    // process can flush them first. See
    // `krabka_observability::wal_group_assignment`.
    let mut assignment = WalAssignmentWatch::new(
        config
            .metrics
            .as_ref()
            .map_or_else(WalConsumerMetrics::unregistered, |metrics| {
                metrics.wal_consumer.clone()
            }),
    );

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
        // Read after the poll, so the snapshot is the one the fetch was served
        // against. An empty poll is observed too: a member that lost every
        // partition returns nothing and would otherwise look idle.
        assignment.observe_consumer(&consumer).await;
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
