//! The retention half of a log block's lifecycle.
//!
//! Compaction decides which WAL records become a block. This module decides
//! which blocks stop being live, and it deletes them. The expiry rule itself
//! is [`krabka_blockstore::plan_expired_blocks`], the one every signal uses,
//! so logs adapts its own index records into
//! [`krabka_blockstore::CompactionCandidate`] rather than holding a second
//! copy of the rule.
//!
//! The sweep runs inside the compactor loop. Shard changes use conditional
//! writes to retain blocks published by other builders after the sweep read
//! its indexes. Legacy full manifests remain whole-object replacements.
//!
//! The index comes first and the objects come second. A reader that lists
//! after the rewrite never learns of the block, so it never asks for the
//! object. The reverse order leaves the index naming an object that is
//! already deleted.

use krabka_blockstore::{
    BlockDeletion, BlockLevel, BlockTimestampUnit, CompactionCandidate, RetentionWindows,
    delete_blocks, list_tenant_log_index_shard_ranges_from_object_store, plan_expired_blocks,
    unescape_object_path_segment, update_tenant_log_index_shard_to_object_store,
};

use crate::{
    BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockStoreError, CompactorRunError,
    LabelIndex, ObjectPath, ObjectStore, TimeRange, insert_descriptor_labels,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    write_tenant_log_index_manifest_to_object_store,
};

mod list_log_index_tenants;
mod log_block_deletion;
mod log_retention_candidates;
mod retire_blocks_from_index;
mod sweep_expired_log_blocks;
mod sweep_expired_tenant_log_blocks;
mod tenant_log_index_shard_ranges;

pub(crate) use list_log_index_tenants::list_log_index_tenants;
pub(crate) use log_block_deletion::log_block_deletion;
pub(crate) use log_retention_candidates::log_retention_candidates;
pub(crate) use retire_blocks_from_index::retire_blocks_from_index;
pub(crate) use sweep_expired_log_blocks::sweep_expired_log_blocks;
pub(crate) use sweep_expired_tenant_log_blocks::sweep_expired_tenant_log_blocks;
pub(crate) use tenant_log_index_shard_ranges::tenant_log_index_shard_ranges;
