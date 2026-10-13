use krabka_observability::persisted_format::validate_consumer_record;

use super::{
    Arc, BTreeMap, BlockIndex, BlockMeta, ConsumerRecord, Labels, ObjectStore, ObjectStoreMetrics,
    OffsetRange, PositionedProfileRecord, ProfileIndex, ProfileRecord, ProfilesError,
    STACKTRACE_PARTITION, build_block_with_positions,
};

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct TenantPartition {
    tenant: String,
    partition: i32,
}

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
    for record in records {
        validate_consumer_record(record).map_err(|error| ProfilesError::Wal(error.to_string()))?;
    }
    let mut batches: BTreeMap<TenantPartition, Vec<PositionedProfileRecord>> = BTreeMap::new();
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
            .entry(TenantPartition {
                tenant: decoded.tenant.clone(),
                partition: record.partition,
            })
            .or_default()
            .push(PositionedProfileRecord::new(
                Some(crate::wal::WalPosition::from_record(record, value)),
                decoded,
            ));
    }

    let mut metas = Vec::new();
    for batch_entry in batches {
        let key = batch_entry.0;
        let mut records = batch_entry.1;
        records.sort_by_key(|record| record.position.map_or(0, |position| position.offset));
        for chunk in records.chunks(flush_records.max(1)) {
            let min_offset = chunk
                .first()
                .and_then(|record| record.position.map(|position| position.offset))
                .unwrap_or_default();
            let max_offset = chunk
                .last()
                .and_then(|record| record.position.map(|position| position.offset))
                .unwrap_or_default();
            let built = build_block_with_positions(
                store,
                &key.tenant,
                key.partition,
                chunk,
                OffsetRange::new(min_offset, max_offset),
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
