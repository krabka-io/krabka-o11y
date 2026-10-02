use std::collections::BTreeSet;

use krabka_metrics::{
    TsdbBlockFiles, TsdbBlockMeta, TsdbImportLimits, TsdbImportOutcome, TsdbImportTarget,
    TsdbPublishError, decode_tsdb_block, publish_tsdb_import, tsdb_block_sha256,
};
use object_store::ObjectStoreExt as _;

use super::{
    Bytes, MimirTenantAdminState, StoredUploadState, UPLOAD_PREFIX, UploadMeta, UploadResult,
    upload_object_key,
};

/// A file of the upload that the store does not hold, or the store failure
/// that stopped its read.
enum ReadError {
    Missing(String),
    Store(String),
}

/// Imports an uploaded Prometheus TSDB block, and returns the upload state
/// that the import ends in.
///
/// A block that is invalid, or whose ULID is bound to other content, ends
/// the upload as failed, and no manifest of it is live. A block whose content
/// is already imported under another ULID ends complete and names that
/// ULID, because its samples are live once.
///
/// # Errors
///
/// Returns the object store failure that stopped the import. The import
/// leaves no manifest of its own, so a retry of the same upload imports the
/// block again.
pub async fn complete_tsdb_upload(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
    meta: &UploadMeta,
) -> Result<StoredUploadState, String> {
    let limits = TsdbImportLimits::default();
    let failed = |message: String| Ok(StoredUploadState::new(UploadResult::Failed, Some(message)));
    // Mimir `validateMaximumBlockSize` adds the size of every entry, also
    // of an entry whose path is repeated.
    let declared = meta
        .thanos
        .files
        .iter()
        .filter(|file| file.rel_path != "meta.json")
        .map(|file| u64::try_from(file.size_bytes).unwrap_or(u64::MAX))
        .fold(0_u64, u64::saturating_add);
    if declared > limits.max_block_bytes {
        return failed(format!(
            "invalid Prometheus TSDB block: the block holds {declared} bytes, and the import \
             accepts at most {}",
            limits.max_block_bytes
        ));
    }

    // Mimir stores one object for each path, so a path that `thanos.files`
    // lists twice names one file. The import reads each path once, in
    // file-name order. A repeated chunk segment would otherwise change the
    // content hash of the same samples.
    let paths: BTreeSet<&str> = meta
        .thanos
        .files
        .iter()
        .map(|file| file.rel_path.as_str())
        .collect();
    let chunk_names: Vec<&str> = paths
        .iter()
        .copied()
        .filter(|path| path.starts_with("chunks/"))
        .collect();
    let has_tombstones = paths.contains("tombstones");
    let (index, chunks, tombstones) =
        match uploaded_files(state, tenant, block, &chunk_names, has_tombstones).await {
            Ok(files) => files,
            Err(ReadError::Missing(name)) => {
                return failed(format!(
                    "invalid Prometheus TSDB block: the file {name} was not uploaded"
                ));
            }
            Err(ReadError::Store(message)) => return Err(message),
        };

    let segments: Vec<&[u8]> = chunks.iter().map(AsRef::as_ref).collect();
    let files = TsdbBlockFiles {
        index: &index,
        chunk_segments: &segments,
        tombstones: tombstones.as_deref(),
    };
    let block_meta = TsdbBlockMeta {
        min_time: meta.min_time,
        max_time: meta.max_time,
        external_labels: meta.thanos.labels.clone(),
    };
    let decoded = match decode_tsdb_block(tenant, &block_meta, files, &limits) {
        Ok(decoded) => decoded,
        Err(error) => return failed(format!("invalid Prometheus TSDB block: {error}")),
    };
    let sha256 = tsdb_block_sha256(files);
    let target = TsdbImportTarget {
        tenant,
        block_ulid: block,
        sha256: &sha256,
        manifest_prefix: &state.query_store.manifest_prefix,
        record_prefix: UPLOAD_PREFIX,
    };
    match publish_tsdb_import(&state.store, target, &decoded).await {
        Ok(TsdbImportOutcome::Imported(_)) => {
            Ok(StoredUploadState::new(UploadResult::Complete, None))
        }
        Ok(TsdbImportOutcome::AlreadyImported(record)) => Ok(StoredUploadState {
            existing_block: (record.ulid != block).then_some(record.ulid),
            ..StoredUploadState::new(UploadResult::Complete, None)
        }),
        Err(
            error @ (TsdbPublishError::Conflict { .. }
            | TsdbPublishError::InvalidTarget(_)
            | TsdbPublishError::InvalidRecord { .. }
            | TsdbPublishError::UnsupportedRecordVersion { .. }
            | TsdbPublishError::Encode(_)),
        ) => failed(format!("Prometheus TSDB block import failed: {error}")),
        Err(error) => Err(format!("Prometheus TSDB block import failed: {error}")),
    }
}

/// Reads the index, the chunk segments in the order of `chunk_names`, and the
/// tombstones if the block has them.
async fn uploaded_files(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
    chunk_names: &[&str],
    has_tombstones: bool,
) -> Result<(Bytes, Vec<Bytes>, Option<Bytes>), ReadError> {
    let index = uploaded_file(state, tenant, block, "index").await?;
    let mut chunks = Vec::with_capacity(chunk_names.len());
    for name in chunk_names {
        chunks.push(uploaded_file(state, tenant, block, name).await?);
    }
    let tombstones = if has_tombstones {
        Some(uploaded_file(state, tenant, block, "tombstones").await?)
    } else {
        None
    };
    Ok((index, chunks, tombstones))
}

async fn uploaded_file(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
    name: &str,
) -> Result<Bytes, ReadError> {
    let key = upload_object_key(tenant, block, &format!("files/{name}"));
    let store_error = |error: object_store::Error| ReadError::Store(format!("read {key}: {error}"));
    match state.store.get(&key).await {
        Ok(object) => object.bytes().await.map_err(store_error),
        Err(object_store::Error::NotFound { .. }) => Err(ReadError::Missing(name.to_owned())),
        Err(error) => Err(store_error(error)),
    }
}
