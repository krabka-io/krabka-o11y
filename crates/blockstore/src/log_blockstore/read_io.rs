use super::*;
use crate::index_snapshot::{list_index_snapshot_objects, snapshot_generation_from_path};

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn read_log_block(
    root: impl AsRef<Path>,
    key: &BlockKey,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let file = File::open(block_path(root, key))?;
    read_log_block_from_reader(file)
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_log_block_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let bytes = store
        .get(&log_block_object_path(prefix, key))
        .await?
        .bytes()
        .await?;
    read_log_block_from_reader(bytes)
}

pub(crate) fn read_log_block_from_reader(
    reader: impl datafusion::parquet::file::reader::ChunkReader + 'static,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(BlockStoreError::UnsupportedBlockFormat)?;
    let reader = builder.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_to_rows(&batch?)?);
    }
    Ok(rows)
}

#[derive(Clone, Debug, Default)]
pub(super) struct LogIndexShardSnapshot {
    pub generation: u64,
    pub indexes: LogIndexes,
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// Reads the newest immutable generation of a tenant's index shard.
///
/// # Errors
/// Returns an error for an absent shard, malformed metadata, failed I/O, or
/// 64 consecutive reads overtaken by snapshot pruning.
pub async fn read_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<LogIndexes, BlockStoreError> {
    let key = log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range);
    read_log_index_shard_snapshot_base(store, &key, tenant)
        .await?
        .map(|snapshot| snapshot.indexes)
        .ok_or_else(|| {
            object_store::Error::NotFound {
                path: key.to_string(),
                source: "log index shard has no published generation".into(),
            }
            .into()
        })
}

pub(super) async fn read_log_index_shard_snapshot_base(
    store: &dyn ObjectStore,
    key: &ObjectPath,
    tenant: &str,
) -> Result<Option<LogIndexShardSnapshot>, BlockStoreError> {
    for _ in 0..64 {
        let Some(meta) = list_index_snapshot_objects(store, key.as_ref())
            .await
            .map_err(log_snapshot_error)?
            .pop()
        else {
            return Ok(None);
        };
        let generation =
            snapshot_generation_from_path(&meta.location).map_err(log_snapshot_error)?;
        match store.get(&meta.location).await {
            Ok(object) => {
                let manifest: LogIndexManifest = serde_json::from_slice(&object.bytes().await?)?;
                let indexes = manifest.into_indexes_for_tenant(tenant)?;
                return Ok(Some(LogIndexShardSnapshot {
                    generation,
                    indexes,
                }));
            }
            // The latest generation in this listing may have been pruned by a
            // publisher. Select again instead of treating the shard as empty.
            Err(object_store::Error::NotFound { .. }) => tokio::task::yield_now().await,
            Err(error) => return Err(error.into()),
        }
    }
    Err(object_store::Error::Generic {
        store: "log index shard",
        source: format!("manifest `{key}` was pruned during 64 consecutive reads").into(),
    }
    .into())
}

pub(super) fn log_snapshot_error(error: crate::BlockStoreError) -> BlockStoreError {
    object_store::Error::Generic {
        store: "log index shard",
        source: Box::new(error),
    }
    .into()
}

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shard_ranges_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_shard_catalog_object_path(prefix, tenant))
        .await?
        .bytes()
        .await?;
    let catalog: LogIndexShardCatalog = serde_json::from_slice(&bytes)?;
    catalog.into_shards()
}

/// Reads the shard catalog of `tenant`, treating an absent catalog as empty.
///
/// A tenant that has never had a catalog written is not a fault, so this
/// returns no ranges for it. A catalog that is present must still be
/// well-formed.
///
/// # Errors
/// Returns the block-store error for every failure except an absent catalog.
pub async fn read_tenant_log_index_shard_ranges_or_empty_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
        Ok(shard_ranges) => Ok(shard_ranges),
        Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

/// Merges every index shard of `tenant` that overlaps `query_range`.
///
/// An absent shard manifest is an empty shard, and so is an absent shard
/// catalog. Retention deletes the manifest of a shard it empties, and the
/// listing or the catalog this read started from can be older than that
/// deletion. A query that races the sweep would otherwise fail on a shard that
/// holds nothing.
///
/// A manifest that is present must still be well-formed. Malformed JSON and a
/// `format_version` this build does not know are errors, so a corrupt index is
/// never read as an empty one.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shards_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<LogIndexes, BlockStoreError> {
    let mut shard_ranges = list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
        store,
        prefix,
        tenant,
        query_range,
    )
    .await?;
    if shard_ranges.is_empty() {
        shard_ranges =
            read_tenant_log_index_shard_ranges_or_empty_from_object_store(store, prefix, tenant)
                .await?;
    }
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for shard_range in shard_ranges
        .into_iter()
        .filter(|shard_range| shard_range.overlaps(query_range))
    {
        let indexes =
            match read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await
            {
                Ok(indexes) => indexes,
                Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => continue,
                Err(error) => return Err(error),
            };

        let LogIndexes { labels, blocks } = indexes;
        for (series_tenant, series) in labels.series {
            for labels in series.into_values() {
                merged_labels.insert_series(series_tenant.clone(), labels);
            }
        }
        for block in blocks.blocks {
            merged_blocks.entry(block.key.object_key()).or_insert(block);
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    Ok(LogIndexes {
        labels: merged_labels,
        blocks: block_index,
    })
}

pub(crate) async fn collect_tenant_log_index_shard_ranges(
    shard_prefix: ObjectPath,
    mut stream: futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>,
    filter_range: Option<TimeRange>,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let mut ranges = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if let Some(range) =
            parse_log_tenant_index_shard_range_from_object_path(&shard_prefix, &meta.location)
            && filter_range.is_none_or(|filter_range| range.overlaps(filter_range))
        {
            ranges.push(range);
        }
    }

    ranges.sort_by(|left, right| {
        left.start_ns
            .cmp(&right.start_ns)
            .then_with(|| left.end_ns.cmp(&right.end_ns))
    });
    ranges.dedup();
    Ok(ranges)
}

/// `head`s one planned block and rejects it above `max_bytes`.
///
/// The object store reports a raw `u64`, so this is the one place that lifts a
/// block's on-disk size into a [`ByteSize`]. The message prints whole bytes, so
/// it reads the same however the cap was chosen.
pub(crate) async fn head_log_block_within_cap(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    max_bytes: ByteSize,
) -> Result<(), BlockStoreError> {
    let meta = store.head(path).await?;
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::LogBlockTooLarge {
            object_key: path.to_string(),
            size_bytes: meta.size,
            max_bytes: max_bytes.bytes_u64(),
        });
    }
    Ok(())
}

use super::{BlockStoreError, ByteSize, ObjectPath, ObjectStore};

/// How many `head` requests the cap pre-pass keeps in flight.
///
/// A planned scan is tens of blocks at most, and each request is one round
/// trip that returns a few hundred bytes. Issuing them one at a time would put
/// the object store's latency on the query's critical path once per block.
const HEAD_CONCURRENCY: usize = 16;

/// Rejects the scan if any planned block is larger than `max_bytes`.
///
/// Blocks come from shared object storage and, per the threat model, may be
/// corrupt or maliciously oversized; this is the same guard `crate::reader`
/// applies before it opens a block for every other signal. The Parquet scan
/// beneath this one is streaming, so an oversized block no longer has to be
/// resident all at once, but its footer, its page index and a single row group
/// still do, and none of those are bounded by anything the reader controls.
/// One `head` per block turns that into an error.
pub(crate) async fn head_log_blocks_within_cap(
    store: &dyn ObjectStore,
    block_paths: &[ObjectPath],
    max_bytes: ByteSize,
) -> Result<(), BlockStoreError> {
    let pending = block_paths
        .iter()
        .map(|path| head_log_block_within_cap(store, path, max_bytes))
        .collect::<Vec<_>>();
    let mut heads = futures::stream::iter(pending).buffer_unordered(HEAD_CONCURRENCY);

    while let Some(result) = heads.next().await {
        result?;
    }
    Ok(())
}

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn list_tenant_log_index_shard_ranges_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let shard_prefix = log_tenant_index_shards_object_prefix(prefix, tenant);
    collect_tenant_log_index_shard_ranges(
        shard_prefix.clone(),
        store.list(Some(&shard_prefix)),
        None,
    )
    .await
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = query_range.start_ns, end_ns = query_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let shard_prefix = log_tenant_index_shards_object_prefix(prefix, tenant);
    collect_tenant_log_index_shard_ranges(
        shard_prefix.clone(),
        // Unpadded signed keys and arbitrarily long shards cannot prove an offset.
        store.list(Some(&shard_prefix)),
        Some(query_range),
    )
    .await
}
