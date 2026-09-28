use super::{
    Arc, BlockWriter, CompactionIndexManifest, CompactionObjectPlan, DecodedTsdbBlock,
    ImportCommit, MetricBlockKind, ObjectStore, ObjectStoreExt, Path, PutPayload, RecordBatch,
    TenantBatches, TsdbImportBinding, TsdbImportKeys, TsdbImportObject, TsdbImportOutcome,
    TsdbImportRecord, TsdbImportTarget, TsdbPublishError, create_import_json,
    encode_tenant_batches, put_import_json, read_import_json, series_labels_for_kind,
};

/// Publishes a decoded TSDB block so that the metrics query path reads it.
///
/// The publication runs in this order:
///
/// 1. It creates the binding of the block ULID to the content hash. A binding
///    to another hash stops the import with [`TsdbPublishError::Conflict`]
///    before any block object is written.
/// 2. It writes one Parquet block per sample kind. No manifest names them
///    yet, so the query path does not read them.
/// 3. It creates the import record of the content hash with a create-only
///    put. The record is the commit point.
/// 4. It writes the `.index` manifest of each block. The manifests make the
///    blocks live.
/// 5. It marks the record published.
///
/// An error before the commit point deletes the blocks and a binding that
/// this call created. An error in step 4 or 5 deletes the manifests that this
/// call wrote. Either way, a failed call leaves no index entry of the import.
/// A process that stops during step 4 can leave some manifests live under an
/// unpublished record. Any later import of the same content, under any ULID,
/// writes the missing manifests and marks the record published.
///
/// If the record is already published, the function writes nothing and
/// returns [`TsdbImportOutcome::AlreadyImported`] with the stored record, so
/// the content is stored once. It does not restore manifests that compaction
/// or retention deleted since.
///
/// # Errors
///
/// Returns [`TsdbPublishError::InvalidTarget`] for a target that does not
/// name a tenant, a ULID and a SHA-256 hash, or whose tenant is not the
/// tenant of the rows, [`TsdbPublishError::Conflict`] when the ULID is bound
/// to other content, and the record, encoding, block and object store errors
/// that stop a step.
pub async fn publish_tsdb_import(
    store: &Arc<dyn ObjectStore>,
    target: TsdbImportTarget<'_>,
    block: &DecodedTsdbBlock,
) -> Result<TsdbImportOutcome, TsdbPublishError> {
    check_target(&target, block)?;
    let keys = TsdbImportKeys::new(target.tenant, target.manifest_prefix, target.record_prefix);
    let binding_key = keys.binding(target.block_ulid);
    let created_binding = bind(store.as_ref(), &binding_key, &target).await?;
    if let Some(record) = read_import_json(store.as_ref(), &keys.record(target.sha256)).await? {
        return resume(
            store,
            &keys,
            &target,
            checked_record(record, &keys, &target)?,
            block,
        )
        .await;
    }

    let mut written = Vec::new();
    match stage_and_commit(store, &keys, &target, block, &mut written).await {
        Ok((ImportCommit::Won(record), manifests)) => {
            publish(store.as_ref(), &keys, record, &manifests)
                .await
                .map(TsdbImportOutcome::Imported)
        }
        Ok((ImportCommit::Lost(existing), _)) => {
            // An import of the same ULID wrote the same keys, which the record
            // names. An import of another ULID wrote keys that no record names.
            if !names_all(&existing, &written) {
                let unnamed = written.iter().map(|key| Path::from(key.as_str()));
                delete_keys(store.as_ref(), unnamed).await;
            }
            resume(
                store,
                &keys,
                &target,
                checked_record(existing, &keys, &target)?,
                block,
            )
            .await
        }
        Err(error) => {
            roll_back(
                store.as_ref(),
                &keys,
                &target,
                &written,
                created_binding.then_some(&binding_key),
            )
            .await;
            Err(error)
        }
    }
}

/// Answers for content that already has a record: a published record as it
/// is, and an unpublished one after this call writes its missing manifests.
async fn resume(
    store: &Arc<dyn ObjectStore>,
    keys: &TsdbImportKeys,
    target: &TsdbImportTarget<'_>,
    record: TsdbImportRecord,
    block: &DecodedTsdbBlock,
) -> Result<TsdbImportOutcome, TsdbPublishError> {
    if record.published {
        return Ok(TsdbImportOutcome::AlreadyImported(record));
    }
    let manifests = missing_manifests(store, &record, block).await?;
    let record = publish(store.as_ref(), keys, record, &manifests).await?;
    // The first import of this content chose the keys. This call only
    // finished it, so the content was there before.
    if record.ulid == target.block_ulid {
        Ok(TsdbImportOutcome::Imported(record))
    } else {
        Ok(TsdbImportOutcome::AlreadyImported(record))
    }
}

fn names_all(record: &TsdbImportRecord, written: &[String]) -> bool {
    written
        .iter()
        .all(|key| record.objects.iter().any(|object| &object.block_key == key))
}

fn check_target(
    target: &TsdbImportTarget<'_>,
    block: &DecodedTsdbBlock,
) -> Result<(), TsdbPublishError> {
    if target.tenant.is_empty() || block.rows.tenant != target.tenant {
        return Err(TsdbPublishError::InvalidTarget(format!(
            "tenant {:?} does not own rows decoded for tenant {:?}",
            target.tenant, block.rows.tenant
        )));
    }
    if target.block_ulid.len() != 26
        || !target
            .block_ulid
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
    {
        return Err(TsdbPublishError::InvalidTarget(format!(
            "block ULID {:?} is not 26 upper-case alphanumeric characters",
            target.block_ulid
        )));
    }
    if target.sha256.len() != 64
        || !target
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(TsdbPublishError::InvalidTarget(format!(
            "content hash {:?} is not 64 lower-case hex digits",
            target.sha256
        )));
    }
    Ok(())
}

/// Creates the binding of the ULID to the content hash, and answers whether
/// this call created it.
async fn bind(
    store: &dyn ObjectStore,
    key: &Path,
    target: &TsdbImportTarget<'_>,
) -> Result<bool, TsdbPublishError> {
    let binding = TsdbImportBinding {
        version: TsdbImportRecord::VERSION,
        ulid: target.block_ulid.to_owned(),
        sha256: target.sha256.to_owned(),
    };
    if create_import_json(store, key, &binding).await? {
        return Ok(true);
    }
    let existing: TsdbImportBinding =
        read_import_json(store, key)
            .await?
            .ok_or_else(|| TsdbPublishError::InvalidRecord {
                key: key.to_string(),
                reason: "the binding was deleted during the import".to_owned(),
            })?;
    if existing.ulid != target.block_ulid {
        return Err(TsdbPublishError::InvalidRecord {
            key: key.to_string(),
            reason: format!("the binding names block {}", existing.ulid),
        });
    }
    if existing.sha256 != target.sha256 {
        return Err(TsdbPublishError::Conflict {
            ulid: target.block_ulid.to_owned(),
            existing: existing.sha256,
            uploaded: target.sha256.to_owned(),
        });
    }
    Ok(false)
}

/// Checks that a stored record describes the target content and names only
/// the keys that an import of that content writes.
fn checked_record(
    record: TsdbImportRecord,
    keys: &TsdbImportKeys,
    target: &TsdbImportTarget<'_>,
) -> Result<TsdbImportRecord, TsdbPublishError> {
    let invalid = |reason: String| TsdbPublishError::InvalidRecord {
        key: keys.record(target.sha256).to_string(),
        reason,
    };
    if record.sha256 != target.sha256 {
        return Err(invalid(format!(
            "the record names content {}",
            record.sha256
        )));
    }
    for object in &record.objects {
        let expected = keys.object(&record.ulid, &record.sha256, object.kind);
        if (object.block_key.as_str(), object.index_key.as_str())
            != (expected.0.as_str(), expected.1.as_str())
        {
            return Err(invalid(format!(
                "the record names object {}, which is not an import key",
                object.block_key
            )));
        }
    }
    Ok(record)
}

/// Writes the blocks, then creates the import record.
///
/// Each block key goes into `written` before its write starts, so that a
/// rollback also reaches an object whose write failed after the store kept
/// it.
async fn stage_and_commit(
    store: &Arc<dyn ObjectStore>,
    keys: &TsdbImportKeys,
    target: &TsdbImportTarget<'_>,
    block: &DecodedTsdbBlock,
    written: &mut Vec<String>,
) -> Result<(ImportCommit, Vec<CompactionIndexManifest>), TsdbPublishError> {
    let mut manifests = Vec::new();
    for (kind, batch) in kind_batches(encode_tenant_batches(&block.rows)?) {
        let (block_key, index_key) = keys.object(target.block_ulid, target.sha256, kind);
        written.push(block_key.clone());
        manifests.push(write_block(store, block, kind, batch, block_key, index_key).await?);
    }

    let record = TsdbImportRecord {
        version: TsdbImportRecord::VERSION,
        ulid: target.block_ulid.to_owned(),
        sha256: target.sha256.to_owned(),
        stats: block.stats,
        objects: manifests
            .iter()
            .map(|manifest| TsdbImportObject {
                kind: manifest.kind,
                block_key: manifest.block_key.clone(),
                index_key: manifest.index_key.clone(),
                rows: u64::try_from(manifest.row_count).unwrap_or(u64::MAX),
            })
            .collect(),
        published: false,
    };
    let record_key = keys.record(target.sha256);
    if create_import_json(store.as_ref(), &record_key, &record).await? {
        return Ok((ImportCommit::Won(record), manifests));
    }
    let existing = read_import_json(store.as_ref(), &record_key)
        .await?
        .ok_or_else(|| TsdbPublishError::InvalidRecord {
            key: record_key.to_string(),
            reason: "the record was deleted during the import".to_owned(),
        })?;
    Ok((ImportCommit::Lost(existing), manifests))
}

/// Writes the blocks of the manifests that an unpublished record names and
/// the store lacks, and returns those manifests.
///
/// The blocks are written again, because no manifest named the old copies,
/// and the orphan sweep may have deleted them.
async fn missing_manifests(
    store: &Arc<dyn ObjectStore>,
    record: &TsdbImportRecord,
    block: &DecodedTsdbBlock,
) -> Result<Vec<CompactionIndexManifest>, TsdbPublishError> {
    let mut missing = Vec::new();
    for object in &record.objects {
        let key = Path::from(object.index_key.as_str());
        match store.head(&key).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => missing.push(object),
            Err(source) => {
                return Err(TsdbPublishError::ObjectStore {
                    key: key.to_string(),
                    source,
                });
            }
        }
    }
    let mut manifests = Vec::new();
    if missing.is_empty() {
        return Ok(manifests);
    }
    let batches = kind_batches(encode_tenant_batches(&block.rows)?);
    for object in missing {
        let batch = batches
            .iter()
            .find(|(kind, _)| *kind == object.kind)
            .map(|(_, batch)| batch.clone())
            .ok_or_else(|| TsdbPublishError::InvalidRecord {
                key: object.index_key.clone(),
                reason: format!("the block holds no {:?} rows", object.kind),
            })?;
        manifests.push(
            write_block(
                store,
                block,
                object.kind,
                batch,
                object.block_key.clone(),
                object.index_key.clone(),
            )
            .await?,
        );
    }
    Ok(manifests)
}

/// Writes `manifests`, then marks the record published.
///
/// If a step fails, the function deletes the manifests that it wrote, so that
/// a failed call leaves no index entry that it made.
async fn publish(
    store: &dyn ObjectStore,
    keys: &TsdbImportKeys,
    record: TsdbImportRecord,
    manifests: &[CompactionIndexManifest],
) -> Result<TsdbImportRecord, TsdbPublishError> {
    let record = TsdbImportRecord {
        published: true,
        ..record
    };
    let record_key = keys.record(&record.sha256);
    let mut written = 0;
    let mut outcome = Ok(());
    for manifest in manifests {
        written += 1;
        outcome = put_manifest(store, manifest).await;
        if outcome.is_err() {
            break;
        }
    }
    if outcome.is_ok() {
        outcome = put_import_json(store, &record_key, &record).await;
    }
    match outcome {
        Ok(()) => Ok(record),
        Err(error) => {
            let keys = manifests[..written]
                .iter()
                .map(|manifest| Path::from(manifest.index_key.as_str()));
            delete_keys(store, keys).await;
            Err(error)
        }
    }
}

fn kind_batches(batches: TenantBatches) -> Vec<(MetricBlockKind, RecordBatch)> {
    [
        (MetricBlockKind::Float, batches.float),
        (MetricBlockKind::NativeHistograms, batches.native_histograms),
    ]
    .into_iter()
    .filter_map(|(kind, batch)| batch.map(|batch| (kind, batch)))
    .collect()
}

async fn write_block(
    store: &Arc<dyn ObjectStore>,
    block: &DecodedTsdbBlock,
    kind: MetricBlockKind,
    batch: RecordBatch,
    block_key: String,
    index_key: String,
) -> Result<CompactionIndexManifest, TsdbPublishError> {
    let meta = BlockWriter::new(store.clone())
        .write_block(&block.rows.tenant, &block_key, batch.schema(), &[batch])
        .await?;
    // An import reads no WAL, so its offset window is empty.
    let plan = CompactionObjectPlan {
        block_key,
        index_key,
        first_offset: 0,
        last_offset: 0,
        row_count: meta.row_count,
    };
    Ok(CompactionIndexManifest::from_block_meta(
        kind,
        &plan,
        &meta,
        series_labels_for_kind(&block.rows, kind),
    ))
}

async fn put_manifest(
    store: &dyn ObjectStore,
    manifest: &CompactionIndexManifest,
) -> Result<(), TsdbPublishError> {
    let key = Path::from(manifest.index_key.as_str());
    store
        .put(&key, PutPayload::from(manifest.encode()?))
        .await
        .map_err(|source| TsdbPublishError::ObjectStore {
            key: key.to_string(),
            source,
        })?;
    Ok(())
}

/// Deletes the blocks of an import that failed before its commit point, then
/// a binding that it created.
///
/// If the import record exists by now, a concurrent import of the same ULID
/// committed the same keys, and the blocks and the binding stay. A delete that fails leaves a
/// block that no manifest names, which the metrics orphan sweep deletes after
/// its grace period, or a binding that a retry of the same content reuses.
async fn roll_back(
    store: &dyn ObjectStore,
    keys: &TsdbImportKeys,
    target: &TsdbImportTarget<'_>,
    written: &[String],
    binding: Option<&Path>,
) {
    let committed = read_import_json::<TsdbImportRecord>(store, &keys.record(target.sha256))
        .await
        .ok()
        .flatten()
        .is_some_and(|record| names_all(&record, written));
    if !committed {
        let blocks = written.iter().map(|key| Path::from(key.as_str()));
        delete_keys(store, blocks.chain(binding.cloned())).await;
    }
}

async fn delete_keys(store: &dyn ObjectStore, keys: impl Iterator<Item = Path>) {
    for key in keys {
        match store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                tracing::warn!(key = %key, error = %error, "a failed TSDB import left an object behind");
            }
        }
    }
}
