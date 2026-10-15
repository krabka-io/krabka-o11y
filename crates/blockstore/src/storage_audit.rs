//! Offline storage audit and report-first repair.
//!
//! [`audit_store`] lists a store once, matches every key against the key
//! grammars of the four signals, and reports what is wrong: missing, corrupt
//! and orphaned blocks, sidecars, index entries, manifests, symbols, delete
//! state, checksums and WAL offset bounds. It only lists and reads.
//!
//! [`repair_store`] acts on an audit. It needs an explicit tenant, signal and
//! finding-kind allowlist, and it plans unless the caller sets `apply`. It
//! deletes only orphaned blocks and orphaned sidecars that are older than the
//! grace window, and it reads the head of each object again before it
//! deletes it. Every other finding needs a person.
//!
//! # Liveness
//!
//! Each signal has its own rule for what makes a block live, and the audit
//! uses the rule of the service that owns the block:
//!
//! - Metrics: an `.index` manifest names the block. The compactor writes the
//!   manifest after the block, and retention deletes the manifest first.
//! - Logs: the global, tenant or shard manifest names the block.
//! - Traces and profiles: the latest index snapshot names the block.
//!
//! When the audit cannot read the index of a tenant, it knows no block of
//! that tenant is an orphan, and it reports none. A wrong index key therefore
//! shows up as an unreadable manifest, not as a store full of orphans.
//!
//! # Formats
//!
//! The audit report, the repair report and the repair audit log carry
//! [`STORAGE_AUDIT_SCHEMA_VERSION`]. Readers refuse another version.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::Write,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::StreamExt;
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
    mebibytes,
};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};
use serde_wincode::{SerdeCompat, wincode::Deserialize as WincodeDeserialize};
use tracing::instrument;

use crate::{
    BlockLevel, BlockStoreError, DEFAULT_BLOCK_READ_MAX, DEFAULT_BLOCK_SWEEP_GRACE,
    DEFAULT_INDEX_SNAPSHOT_MAX, ErasureRequest, IndexShardRange, Labels, LogBlockIndex,
    LogBlockStoreError, MERGE_READ_BATCH_ROWS, ProfileIndex, TimeRange, TraceIndex,
    escape_object_path_segment, index_shards_prefix_for_key,
    index_snapshot::{
        TenantSnapshotRangeRead, list_index_snapshot_objects, read_latest_snapshot_manifest,
        read_shard_payload, shard_payload_content_hash, shard_payload_object_key,
        shard_payload_prefix_for_key,
    },
    index_snapshot_prefix_for_key, log_tenant_index_manifest_object_path,
    log_tenant_index_shard_manifest_object_path, open_block_stream,
    read_log_index_manifest_from_object_store, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    reader::block_metadata,
    unescape_object_path_segment,
};

mod audit_delete_state;
mod audit_inventory;
mod audit_log_manifests;
mod audit_snapshot_index;
mod check_block;
mod classified_object;
mod classify_object_key;
mod compare_with_live;
mod live_block_set;
mod log_frontier_manifest;
mod metrics_index_manifest;
mod repair_log_entry;
mod repair_log_writer;
mod repair_options;
mod repair_report;
mod repair_store;
mod storage_audit_options;
mod storage_audit_report;
mod storage_finding_kind;
mod storage_inventory;
mod storage_signal;
mod symbol_table_shape;

use self::{
    audit_delete_state::audit_delete_state,
    audit_inventory::audit_inventory,
    audit_log_manifests::audit_log_manifests,
    audit_snapshot_index::audit_snapshot_index,
    check_block::check_block,
    classified_object::ClassifiedObject,
    classify_object_key::classify_object_key,
    compare_with_live::{append_unindexed_blocks, compare_with_live},
    live_block_set::LiveBlockSet,
    log_frontier_manifest::LogFrontierManifest,
    metrics_index_manifest::MetricsIndexManifest,
    storage_inventory::StorageInventory,
    symbol_table_shape::SymbolTableShape,
};
pub use self::{
    repair_log_entry::RepairLogEntry, repair_log_writer::RepairLogWriter,
    repair_options::RepairOptions, repair_report::RepairReport, repair_store::repair_store,
    storage_audit_options::StorageAuditOptions, storage_audit_report::StorageAuditReport,
    storage_finding_kind::StorageFindingKind, storage_signal::StorageSignal,
};

/// What a block key says about the block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockShape {
    /// The WAL partition, when the key names one.
    pub partition: Option<i32>,
    /// The metrics block kind, or an empty string for other signals.
    pub lane: String,
    /// The inclusive WAL offset range, when the key names one.
    pub offsets: Option<crate::OffsetRange>,
    /// The inclusive record time range in nanoseconds, when the key names one.
    pub time_range: Option<crate::log_blockstore::TimeRange>,
}

/// One object from the listing, with what its key says about it.
#[derive(Clone, Debug)]
pub struct ListedObject {
    pub meta: ObjectMeta,
    pub object: ClassifiedObject,
}

/// The block kind of a metrics `.index` manifest, in persisted variant order.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum MetricsBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
    ClockReadings,
}

/// The part an object plays in its signal's layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectRole {
    /// A Parquet block.
    Block(BlockShape),
    /// A metrics `.index` sidecar that makes its block live.
    MetricsIndex { block: String },
    /// A profiles `.symdb` symbol table, written after its block.
    Symbols { block: String },
    /// A generation, payload or shard object of a snapshot index.
    IndexSnapshot,
    /// `tenant=…/index/logs/manifest.json`.
    LogTenantManifest,
    /// `tenant=…/index/logs/shards/manifest.json`.
    LogShardCatalog,
    /// A JSON generation under `tenant=…/index/logs/shards/time=S-E/manifest/snapshots`.
    LogShardManifest { start_ns: i64, end_ns: i64 },
    /// A listing aid under `tenant=…/index/logs/shards/time=N`.
    LogShardListOffset,
    /// `index/logs/manifest.json`.
    LogGlobalManifest,
    /// `index/logs/compaction-frontier.json`.
    LogFrontier,
    /// `mimir-tenant-deletions/{tenant}.json`.
    TenantDeletion,
    /// `metric-erasure-requests/{tenant}/{id}.json`.
    ErasureRequest { id: String },
    /// An object the audit knows and does not check, such as staging data.
    Staging,
}

/// One object a repair acted on.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairAction {
    pub kind: StorageFindingKind,
    pub path: String,
    pub outcome: RepairOutcome,
    /// Why the repair skipped the object or failed, for a person to read.
    pub detail: Option<String>,
}

/// Which of the two lines of a delete a repair log line is.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairLogPhase {
    /// The repair is about to delete the object; an unmatched intent may mean
    /// the delete happened.
    Intent,
    /// What the repair did with the object.
    Outcome,
}

/// What a repair did with one finding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairOutcome {
    /// The repair ran without `apply` and would delete the object.
    Planned,
    /// The repair deleted the object.
    Deleted,
    /// The object was already gone when the repair reached it.
    AlreadyAbsent,
    /// The object changed after audit, so the repair left it in place.
    SkippedChanged,
    /// The recheck or delete failed; a later run can retry it.
    Failed,
}

/// Version of storage audit reports, repair reports and repair audit logs.
pub const STORAGE_AUDIT_SCHEMA_VERSION: u32 = 1;

/// What an audit looked at, recorded in its report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageAuditScope {
    /// The one signal audited, or `None` for all four.
    pub signal: Option<StorageSignal>,
    /// The one tenant audited, or `None` for every tenant.
    pub tenant: Option<String>,
    /// Grace window in whole seconds before an unindexed object is an orphan.
    pub grace_secs: u64,
    /// Whether the audit read every block in full, not only its footer.
    pub verify_data: bool,
}

/// How much a finding says about the health of the store.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageFindingSeverity {
    /// The store is damaged, or holds an object that no reader will use.
    Damage,
    /// The state is legal but needs a person to look at it.
    Warning,
}

/// A Mimir tenant deletion marker, as persisted by `krabka-metrics-service`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantDeletionMarker {
    pub tenant_id: String,
    pub objects: Vec<String>,
}

/// Where `krabka-observability` persists the logs compaction frontier.
pub const LOG_FRONTIER_PATH: &str = "index/logs/compaction-frontier.json";

/// The largest delete marker, erasure request or frontier object the audit
/// reads. Each one is a small JSON document, so a larger object is damage.
pub const MAX_STATE_OBJECT_BYTES: ByteSize = mebibytes(16);

/// Decodes the `.symdb` symbol table at `key` the way `SymbolDb::decode`
/// does, and returns why it does not decode.
///
/// Returns `None` for a symbol table that decodes or that is gone.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence, or the object is larger than
/// [`DEFAULT_BLOCK_READ_MAX`].
pub async fn check_symbol_table(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<String>, StorageAuditError> {
    let Some(bytes) = read_capped_object(store, key, DEFAULT_BLOCK_READ_MAX).await? else {
        return Ok(None);
    };
    Ok(
        <SerdeCompat<SymbolTableShape> as WincodeDeserialize>::deserialize(&bytes)
            .err()
            .map(|error| error.to_string()),
    )
}

/// Whether `meta` was last written at or before `now - grace`.
///
/// The cutoff matches [`reconcile_orphans`](crate::reconcile_orphans):
/// object timestamps are whole seconds, and a zero grace means every object
/// is old.
pub fn is_older_than_grace(meta: &ObjectMeta, grace: Time, now: SystemTime) -> bool {
    now.checked_sub(grace.to_std())
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_secs()).ok())
        .is_some_and(|cutoff| meta.last_modified.timestamp() <= cutoff)
}

/// The finding for an index or manifest that did not read.
///
/// A version this build does not read is `unsupported_format`. Anything else
/// is `unreadable_manifest`.
pub fn manifest_finding(
    signal: StorageSignal,
    tenant: Option<String>,
    path: impl Into<String>,
    detail: &str,
) -> StorageFinding {
    let unsupported = detail.contains("manifest version");
    let kind = if unsupported {
        StorageFindingKind::UnsupportedFormat
    } else {
        StorageFindingKind::UnreadableManifest
    };
    StorageFinding::new(kind, signal, tenant, path, detail)
}

/// Reads an object of at most `max_bytes`, or `None` when it is gone.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence, or the object is larger than `max_bytes`.
pub async fn read_capped_object(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
) -> Result<Option<Bytes>, StorageAuditError> {
    match krabka_object_store::v013::read_capped(store, &Path::from(key), max_bytes.bytes_u64())
        .await
    {
        Ok(bytes) => Ok(Some(bytes)),
        Err(krabka_object_store::v013::ObjectStoreError::NotFound(_)) => Ok(None),
        Err(error) => Err(StorageAuditError::ObjectStore(error.to_string())),
    }
}

/// Reads a small state object, or `None` when it is gone.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence, or the object is larger than
/// [`MAX_STATE_OBJECT_BYTES`].
pub async fn read_small_object(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<Bytes>, StorageAuditError> {
    read_capped_object(store, key, MAX_STATE_OBJECT_BYTES).await
}

/// The finding for a block that nothing makes live: `orphan` once it is
/// older than the grace window, `pending` before.
pub fn unindexed_block_finding(
    key: &str,
    listed: &ListedObject,
    options: &StorageAuditOptions,
    what: &str,
) -> StorageFinding {
    let kind = if is_older_than_grace(&listed.meta, options.grace, options.now) {
        StorageFindingKind::Orphan
    } else {
        StorageFindingKind::Pending
    };
    StorageFinding::new(
        kind,
        listed.object.signal,
        listed.object.tenant.clone(),
        key,
        format!("no {what} names this block"),
    )
}

struct BlockOverlapRecord {
    tenant: Option<String>,
    partition: Option<i32>,
    lane: String,
    offsets: crate::OffsetRange,
    object_key: String,
}

/// Finds pairs of blocks that cover overlapping WAL offsets.
///
/// Two blocks of one signal, tenant, partition and lane whose inclusive
/// offset ranges meet and differ both hold the records of the overlap. Two
/// writers on one partition do that, and so does a replay after a crash that
/// cut a different window. Queries deduplicate the rows, so the kind is a
/// warning. Two blocks with the same range are one window written twice
/// under two keys and are not reported. A block with no offsets in its key,
/// such as a compacted block, takes no part.
pub fn find_wal_overlaps(
    inventory: &StorageInventory,
    signal: StorageSignal,
) -> Vec<StorageFinding> {
    let mut blocks: Vec<_> = inventory
        .blocks(signal)
        .filter_map(|entry| {
            let key = entry.key;
            let listed = entry.listed;
            let block = listed.object.block()?;
            Some(BlockOverlapRecord {
                tenant: listed.object.tenant.clone(),
                partition: block.partition,
                lane: block.lane.clone(),
                offsets: block.offsets?,
                object_key: key.clone(),
            })
        })
        .collect();
    blocks.sort_by(|left, right| {
        left.tenant
            .cmp(&right.tenant)
            .then_with(|| left.partition.cmp(&right.partition))
            .then_with(|| left.lane.cmp(&right.lane))
            .then_with(|| left.offsets.min.cmp(&right.offsets.min))
            .then_with(|| left.offsets.max.cmp(&right.offsets.max))
            .then_with(|| left.object_key.cmp(&right.object_key))
    });
    let mut findings = Vec::new();
    for lane in blocks.chunk_by(|left, right| {
        left.tenant == right.tenant && left.partition == right.partition && left.lane == right.lane
    }) {
        for at in 0..lane.len() {
            let block = &lane[at];
            for other in &lane[at + 1..] {
                if other.offsets.min > block.offsets.max {
                    break;
                }
                if block.offsets == other.offsets {
                    continue;
                }
                let partition = block
                    .partition
                    .map_or_else(|| "-".to_string(), |p| p.to_string());
                findings.push(StorageFinding::new(
                    StorageFindingKind::WalOverlap,
                    signal,
                    block.tenant.clone(),
                    &block.object_key,
                    format!(
                        "offsets {}-{} of partition {partition} overlap offsets {}-{} of `{}`",
                        block.offsets.min,
                        block.offsets.max,
                        other.offsets.min,
                        other.offsets.max,
                        other.object_key,
                    ),
                ));
            }
        }
    }
    findings
}

/// Reads every metrics `.index` manifest and checks the metrics blocks
/// against them.
///
/// A metrics block is live when a manifest names it, as the metrics
/// retention pass decides. The compactor writes the manifest after the
/// block, and retention deletes it before the block, so a block that no
/// manifest names is `orphan` or `pending`. A manifest whose block is gone is
/// `dangling_index_entry`: the querier would plan a scan of a block that is
/// gone.
///
/// A manifest that does not decode is `unreadable_manifest`. A manifest that
/// names another index key, block or tenant than its key is
/// `index_mismatch`. Either one makes the tenant of the manifest unknown, so
/// the audit calls none of its blocks an orphan. The block beside the
/// manifest and the block the manifest names both stay live.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when a read fails for a reason
/// other than absence, or a manifest is larger than
/// [`DEFAULT_BLOCK_READ_MAX`].
pub async fn audit_metrics_objects(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
    options: &StorageAuditOptions,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut live = LiveBlockSet::default();
    let mut findings = Vec::new();
    for entry in inventory.of_signal(StorageSignal::Metrics) {
        let key = entry.key;
        let listed = entry.listed;
        let ObjectRole::MetricsIndex { block } = &listed.object.role else {
            continue;
        };
        let tenant = listed.object.tenant.clone();
        let finding = |kind, detail: String| {
            StorageFinding::new(kind, StorageSignal::Metrics, tenant.clone(), key, detail)
        };
        // Removed since the listing: retention does that.
        let Some(bytes) = read_capped_object(store, key, DEFAULT_BLOCK_READ_MAX).await? else {
            continue;
        };
        live.live
            .insert(block.clone(), tenant.clone().unwrap_or_default());
        let problem = match MetricsIndexManifest::decode(&bytes) {
            Ok(manifest) => {
                if !inventory.contains(&manifest.block_key) {
                    findings.push(finding(
                        StorageFindingKind::DanglingIndexEntry,
                        format!(
                            "names block `{}`, which the store does not hold",
                            manifest.block_key
                        ),
                    ));
                }
                let mismatch = manifest.mismatch(key, block, tenant.as_deref());
                live.live.insert(manifest.block_key, manifest.tenant);
                mismatch.map(|detail| finding(StorageFindingKind::IndexMismatch, detail))
            }
            Err(error) => Some(finding(StorageFindingKind::UnreadableManifest, error)),
        };
        if let Some(problem) = problem {
            match &tenant {
                Some(tenant) => {
                    live.unknown_tenants.insert(tenant.clone());
                }
                None => live.all_unknown = true,
            }
            findings.push(problem);
        }
    }
    append_unindexed_blocks(
        &mut findings,
        inventory,
        StorageSignal::Metrics,
        &live,
        options,
        "`.index` manifest",
    );
    Ok(findings)
}

/// The block keys that the snapshot index at `key` names for `tenant`.
///
/// # Errors
/// Returns the loader's error when a payload of the tenant does not read or
/// decode.
pub async fn snapshot_tenant_blocks(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    key: &str,
    tenant: &str,
    max_bytes: ByteSize,
) -> Result<Vec<String>, BlockStoreError> {
    let whole_tenant = TenantSnapshotRangeRead {
        store,
        key,
        tenant,
        span: IndexShardRange::new(i64::MIN, i64::MAX),
        max_bytes,
    };
    match signal {
        StorageSignal::Traces => {
            let index =
                TraceIndex::load_latest_snapshot_for_range_with_max_bytes(whole_tenant).await?;
            Ok(index
                .trace_blocks(tenant)
                .iter()
                .map(|block| block.object_key.clone())
                .collect())
        }
        StorageSignal::Profiles => {
            let index =
                ProfileIndex::load_latest_snapshot_for_range_with_max_bytes(whole_tenant).await?;
            Ok(index
                .all_blocks()
                .into_iter()
                .filter(|block| block.tenant == tenant)
                .map(|block| block.object_key)
                .collect())
        }
        StorageSignal::Metrics | StorageSignal::Logs => Ok(Vec::new()),
    }
}

/// Audits a store and reports what is wrong with it. Reads only.
///
/// The audit lists the store once and matches every key against the key
/// grammars of the four signals. It then checks, for each signal in scope:
///
/// - that every block has a Parquet footer this build reads, and with
///   `verify_data`, rows that decode;
/// - that the index names every block, and that every block it names exists;
/// - that every metrics `.index` manifest decodes and names its own key,
///   block and tenant;
/// - that every live profiles block has its `.symdb` symbol table, and with
///   `verify_data`, that the symbol table decodes;
/// - that the index of each tenant names only blocks of that tenant;
/// - that the trace and profile snapshot payloads match their checksums;
/// - that the logs manifests and shard catalogs decode and agree with the
///   store;
/// - that metrics delete markers and erasure requests decode;
/// - that no two blocks of one WAL partition cover overlapping offsets, and
///   that the logs compaction frontier is not ahead of every block.
///
/// `store` is the root that the services write to, such as a
/// [`PrefixStore`](object_store::prefix::PrefixStore) over the bucket prefix.
///
/// # Errors
/// Returns [`StorageAuditError`] when the listing fails or a read fails for
/// a reason other than a damaged object. The audit then knows too little to
/// report, and a partial report would read as a clean one.
#[instrument(level = "info", skip_all, err)]
pub async fn audit_store(
    store: &Arc<dyn ObjectStore>,
    options: &StorageAuditOptions,
) -> Result<StorageAuditReport, StorageAuditError> {
    audit_inventory(store, options)
        .await
        .map(|result| result.report)
}

/// Errors that stop a storage audit or a repair pass.
///
/// A damaged object is not one of them: the audit reports it as a
/// [`StorageFinding`](StorageFinding) and carries on. Only a failure
/// that leaves the pass unable to tell what exists reaches here.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageAuditError {
    /// Listing or reading the store failed, so the audit does not know what
    /// the store holds.
    #[error("object store error: {0}")]
    ObjectStore(String),
    /// A reader failed for a reason that is not the fault of one object.
    #[error("block store error: {0}")]
    BlockStore(String),
    /// The requested scope is not one a repair accepts.
    #[error("invalid repair scope: {0}")]
    InvalidScope(String),
    /// A report was written by a build that uses another schema version.
    #[error("unsupported storage audit schema version {actual}; expected {expected}")]
    UnsupportedSchemaVersion { actual: u32, expected: u32 },
    /// A report or a log line does not decode.
    #[error("invalid storage audit report: {0}")]
    InvalidReport(String),
    /// Writing the repair audit log failed.
    #[error("audit log write failed: {0}")]
    AuditLog(String),
}

impl From<object_store::Error> for StorageAuditError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}

/// One thing an audit found wrong with one object.
///
/// Findings sort by kind, then signal, tenant, path and detail, so two audits
/// of one store list them in one order.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct StorageFinding {
    pub kind: StorageFindingKind,
    pub signal: StorageSignal,
    /// The tenant that owns the object. `None` marks shared state, such as a
    /// fleet-wide index or the logs compaction frontier.
    pub tenant: Option<String>,
    /// The object key, relative to the audited prefix.
    pub path: String,
    /// What the audit saw, for a person to read. Not a stable contract.
    pub detail: String,
    /// Copied from the kind, so a reader of the report does not need a table.
    pub severity: StorageFindingSeverity,
    /// Copied from the kind. Only a repairable finding can be deleted, and
    /// only when a repair names its kind.
    pub repairable: bool,
}

impl StorageFinding {
    /// A finding of `kind`, with the severity and repairability that `kind`
    /// implies.
    #[must_use]
    pub fn new(
        kind: StorageFindingKind,
        signal: StorageSignal,
        tenant: Option<String>,
        path: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            signal,
            tenant,
            path: path.into(),
            detail: detail.into(),
            severity: kind.severity(),
            repairable: kind.is_repairable(),
        }
    }
}

/// Compares the logs compaction frontier with the log blocks of each WAL
/// partition.
///
/// A frontier offset above the last offset of every retained block of its
/// partition is `stale_frontier`. So is a `compacted_through_ns` after the
/// newest record time of every retained block. The querier trusts the
/// frontier to say which WAL records are in blocks, so it hides records from
/// the hot tail that no block holds. A batch that the delete filters emptied
/// does this legally, which is why the kind is a warning.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence.
pub async fn check_log_frontier(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    if !inventory.contains(LOG_FRONTIER_PATH) {
        return Ok(Vec::new());
    }
    let Some(bytes) = read_small_object(store, LOG_FRONTIER_PATH).await? else {
        return Ok(Vec::new());
    };
    let unreadable = |kind, detail: String| {
        Ok(vec![StorageFinding::new(
            kind,
            StorageSignal::Logs,
            None,
            LOG_FRONTIER_PATH,
            detail,
        )])
    };
    let frontier: LogFrontierManifest = match serde_json::from_slice(&bytes) {
        Ok(frontier) => frontier,
        Err(error) => return unreadable(StorageFindingKind::UnreadableManifest, error.to_string()),
    };
    if frontier.version != LogFrontierManifest::VERSION {
        return unreadable(
            StorageFindingKind::UnsupportedFormat,
            format!(
                "compaction frontier manifest version {}, expected {}",
                frontier.version,
                LogFrontierManifest::VERSION
            ),
        );
    }
    let mut last_offsets: BTreeMap<i32, i64> = BTreeMap::new();
    let mut newest_ns: Option<i64> = None;
    for entry in inventory.blocks(StorageSignal::Logs) {
        let listed = entry.listed;
        let Some(block) = listed.object.block() else {
            continue;
        };
        if let Some(partition) = block.partition
            && let Some(offsets) = block.offsets
        {
            let entry = last_offsets.entry(partition).or_insert(offsets.max);
            *entry = (*entry).max(offsets.max);
        }
        if let Some(time_range) = block.time_range {
            let end_ns = time_range.end_ns;
            newest_ns = Some(newest_ns.map_or(end_ns, |newest| newest.max(end_ns)));
        }
    }
    let stale = |detail: String| {
        StorageFinding::new(
            StorageFindingKind::StaleFrontier,
            StorageSignal::Logs,
            None,
            LOG_FRONTIER_PATH,
            detail,
        )
    };
    let mut findings: Vec<StorageFinding> = frontier
        .partition_offsets
        .keys()
        .filter_map(|partition| {
            let frontier_offset = frontier.partition_offsets[partition];
            let last = last_offsets.get(partition)?;
            (frontier_offset > *last).then(|| {
                stale(format!(
                    "partition {partition} is compacted through offset {frontier_offset}, \
                     and its newest block ends at offset {last}"
                ))
            })
        })
        .collect();
    if let Some(newest) = newest_ns
        && frontier.compacted_through_ns > newest
    {
        findings.push(stale(format!(
            "the frontier is compacted through {} ns, and the newest block ends at {newest} ns",
            frontier.compacted_through_ns
        )));
    }
    Ok(findings)
}

struct ProfileSymbolFindingDetails {
    kind: StorageFindingKind,
    detail: String,
}

/// Checks profile blocks against their `.symdb` symbol tables.
///
/// The block builder and the compactor both write the symbol table after the
/// block and before the index names the block. A live block without one
/// cannot be symbolized: `missing_sidecar`. A symbol table whose block is
/// gone, or whose block is an old orphan, is `orphan_sidecar` once it is old
/// too, and `pending` before.
///
/// With `verify_data`, the audit also decodes the symbol table of each live
/// block in scope. A symbol table that does not decode is `corrupt_sidecar`:
/// the querier cannot symbolize the block. The decode reads the whole
/// object, as `verify_data` does for blocks. A damaged symbol table does not
/// change which blocks are live, so a plain audit and a repair do not need
/// it.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when a read fails for a reason
/// other than absence.
pub async fn audit_profile_symbols(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
    live: &LiveBlockSet,
    options: &StorageAuditOptions,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut findings = Vec::new();
    let old = |key: &str| {
        inventory
            .meta(key)
            .is_some_and(|meta| is_older_than_grace(meta, options.grace, options.now))
    };
    for entry in inventory.of_signal(StorageSignal::Profiles) {
        let key = entry.key;
        let listed = entry.listed;
        let tenant = listed.object.tenant.clone();
        match &listed.object.role {
            ObjectRole::Symbols { block } => {
                if live.contains(block) {
                    continue;
                }
                let block_present = inventory.contains(block);
                if block_present && !live.knows(tenant.as_deref()) {
                    continue;
                }
                let details = if old(key) && (!block_present || old(block)) {
                    let detail = if block_present {
                        format!("block `{block}` is an orphan")
                    } else {
                        format!("block `{block}` is gone")
                    };
                    ProfileSymbolFindingDetails {
                        kind: StorageFindingKind::OrphanSidecar,
                        detail,
                    }
                } else {
                    ProfileSymbolFindingDetails {
                        kind: StorageFindingKind::Pending,
                        detail: format!("block `{block}` is not in the index yet"),
                    }
                };
                findings.push(StorageFinding::new(
                    details.kind,
                    StorageSignal::Profiles,
                    tenant,
                    key,
                    details.detail,
                ));
            }
            ObjectRole::Block(_) if live.contains(key) => {
                let symbols = format!("{key}.symdb");
                if !inventory.contains(&symbols) {
                    findings.push(StorageFinding::new(
                        StorageFindingKind::MissingSidecar,
                        StorageSignal::Profiles,
                        tenant,
                        key,
                        format!("the index names this block and `{symbols}` is gone"),
                    ));
                } else if options.verify_data
                    && options.covers_tenant(tenant.as_deref())
                    && let Some(detail) = check_symbol_table(store, &symbols).await?
                {
                    findings.push(StorageFinding::new(
                        StorageFindingKind::CorruptSidecar,
                        StorageSignal::Profiles,
                        tenant,
                        symbols,
                        detail,
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(findings)
}
