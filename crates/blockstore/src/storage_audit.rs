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
//! - Metrics: the block has its `.index` sidecar. The compactor writes the
//!   sidecar after the block, and retention deletes the sidecar first.
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
use krabka_units::{ByteSize, Time, convert::TimeExt};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    BlockStoreError, DEFAULT_BLOCK_READ_MAX, DEFAULT_BLOCK_SWEEP_GRACE, DEFAULT_INDEX_SNAPSHOT_MAX,
    ErasureRequest, LogBlockIndex, LogBlockStoreError, MERGE_READ_BATCH_ROWS, ProfileIndex,
    TimeRange, TraceIndex, escape_object_path_segment, index_shards_prefix_for_key,
    index_snapshot::{
        list_index_snapshot_objects, read_latest_snapshot_manifest, read_shard_payload,
        shard_payload_content_hash, shard_payload_object_key, shard_payload_prefix_for_key,
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
mod audit_metrics_objects;
mod audit_profile_symbols;
mod audit_snapshot_index;
mod audit_store;
mod block_shape;
mod check_block;
mod check_log_frontier;
mod classified_object;
mod classify_object_key;
mod compare_with_live;
mod find_wal_overlaps;
mod is_older_than_grace;
mod listed_object;
mod live_block_set;
mod log_frontier_manifest;
mod log_frontier_path;
mod manifest_finding;
mod max_state_object_bytes;
mod object_role;
mod read_small_object;
mod repair_action;
mod repair_log_entry;
mod repair_options;
mod repair_outcome;
mod repair_report;
mod repair_store;
mod snapshot_tenant_blocks;
mod storage_audit_error;
mod storage_audit_options;
mod storage_audit_report;
mod storage_audit_schema_version;
mod storage_audit_scope;
mod storage_finding;
mod storage_finding_kind;
mod storage_finding_severity;
mod storage_inventory;
mod storage_signal;
mod tenant_deletion_marker;
mod unindexed_block_finding;

use self::{
    audit_delete_state::audit_delete_state, audit_inventory::audit_inventory,
    audit_log_manifests::audit_log_manifests, audit_metrics_objects::audit_metrics_objects,
    audit_profile_symbols::audit_profile_symbols, audit_snapshot_index::audit_snapshot_index,
    block_shape::BlockShape, check_block::check_block, check_log_frontier::check_log_frontier,
    classified_object::ClassifiedObject, classify_object_key::classify_object_key,
    compare_with_live::compare_with_live, find_wal_overlaps::find_wal_overlaps,
    is_older_than_grace::is_older_than_grace, listed_object::ListedObject,
    live_block_set::LiveBlockSet, log_frontier_manifest::LogFrontierManifest,
    log_frontier_path::LOG_FRONTIER_PATH, manifest_finding::manifest_finding,
    max_state_object_bytes::MAX_STATE_OBJECT_BYTES, object_role::ObjectRole,
    read_small_object::read_small_object, snapshot_tenant_blocks::snapshot_tenant_blocks,
    storage_inventory::StorageInventory, tenant_deletion_marker::TenantDeletionMarker,
    unindexed_block_finding::unindexed_block_finding,
};
pub use self::{
    audit_store::audit_store, repair_action::RepairAction, repair_log_entry::RepairLogEntry,
    repair_options::RepairOptions, repair_outcome::RepairOutcome, repair_report::RepairReport,
    repair_store::repair_store, storage_audit_error::StorageAuditError,
    storage_audit_options::StorageAuditOptions, storage_audit_report::StorageAuditReport,
    storage_audit_schema_version::STORAGE_AUDIT_SCHEMA_VERSION,
    storage_audit_scope::StorageAuditScope, storage_finding::StorageFinding,
    storage_finding_kind::StorageFindingKind, storage_finding_severity::StorageFindingSeverity,
    storage_signal::StorageSignal,
};
