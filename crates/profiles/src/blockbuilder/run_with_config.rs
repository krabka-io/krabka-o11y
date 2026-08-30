use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run_with_config(config: BlockBuilderConfig) -> Result<(), ProfilesError> {
    let mut index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &config.store,
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
        .group_instance_id(config.group_id)
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
        let records = consumer
            .poll(config.poll_timeout)
            .await
            .map_err(|err| ProfilesError::Block(format!("consumer poll failed: {err}")))?;
        let now = Instant::now();
        accumulator.push(records, now);
        if !accumulator.should_flush(now) {
            continue;
        }
        let records = accumulator.take();
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
            )
            .await?;
            if let Some(metrics) = &config.metrics {
                metrics.record_blocks_built(metas.len() as u64);
            }
            index
                .save_latest_snapshot_with_retain(
                    &config.store,
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
    }
}
