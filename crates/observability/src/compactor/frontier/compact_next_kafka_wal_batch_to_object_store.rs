use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_next_kafka_wal_batch_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
) -> Result<Option<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    if records.is_empty() {
        return Ok(None);
    }

    let mut committer = LastCompactedPosition::default();
    let descriptor = compact_kafka_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        &mut committer,
        records,
    )
    .await?;
    let position = committer
        .position
        .ok_or(CompactorRunError::MissingCommitPosition)?;
    consumer.commit_compacted(position).await?;

    Ok(Some(descriptor))
}
