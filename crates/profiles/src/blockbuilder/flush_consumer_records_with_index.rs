use super::{
    Arc, BTreeMap, BlockIndex, BlockMeta, ConsumerRecord, Labels, ObjectStore, ObjectStoreMetrics,
    ProfileIndex, ProfileRecord, ProfilesError, STACKTRACE_PARTITION, build_block,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn flush_consumer_records_with_index(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    records: &[ConsumerRecord],
    flush_records: usize,
    metrics: &ObjectStoreMetrics,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let mut batches: BTreeMap<(String, i32), Vec<(i64, ProfileRecord)>> = BTreeMap::new();
    for record in records {
        let value = record
            .value
            .as_deref()
            .ok_or_else(|| ProfilesError::Wal("profiles WAL record has no value".to_string()))?;
        let decoded = ProfileRecord::decode(value)?;
        let labels = Labels::from_pairs(decoded.labels.iter().cloned());
        // The index refuses a series that carries no `__profile_type__`, and
        // the flush stops with it rather than writing a block whose series no
        // profile-type selector can reach. Ingest's split stamps the label on
        // every series it emits, so this is a fault in whoever produced the
        // record, and the flush must not advance past it in silence.
        index
            .add_series(&decoded.tenant, labels.fingerprint(), &labels)
            .map_err(|error| ProfilesError::Block(error.to_string()))?;
        batches
            .entry((decoded.tenant.clone(), record.partition))
            .or_default()
            .push((record.offset, decoded));
    }

    let mut metas = Vec::new();
    for ((tenant, partition), mut records) in batches {
        records.sort_by_key(|(offset, _)| *offset);
        for chunk in records.chunks(flush_records.max(1)) {
            let min_offset = chunk.first().map(|(offset, _)| *offset).unwrap_or_default();
            let max_offset = chunk.last().map(|(offset, _)| *offset).unwrap_or_default();
            let profile_records = chunk
                .iter()
                .map(|(_, record)| record.clone())
                .collect::<Vec<_>>();
            let built = build_block(
                store,
                &tenant,
                partition,
                &profile_records,
                (min_offset, max_offset),
                metrics,
            )
            .await?;
            for meta in &built {
                index.add_block(meta);
                index.add_profile_block(&meta.tenant, &meta.object_key, vec![STACKTRACE_PARTITION]);
            }
            metas.extend(built);
        }
    }
    Ok(metas)
}
