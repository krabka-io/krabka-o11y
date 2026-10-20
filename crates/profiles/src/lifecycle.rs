//! What happens to a profile block after it is written.
//!
//! A block is written once and then only ever removed. Three things remove
//! one, and all three run in the compactor pass that
//! [`run_lifecycle_pass`] is:
//!
//! - A merge retires its inputs. The compactor writes one block in their place
//!   and the inputs become unreachable.
//! - Retention expires a block that ends before its tenant's window. See
//!   [`RetentionWindows`], which the
//!   profiles [`OverridesProvider`](crate::limits::OverridesProvider)
//!   implements from the `compactor_blocks_retention_period` limit.
//! - The orphan sweep deletes an object the index never named, or stopped
//!   naming while a pass was interrupted.
//!
//! # The symbol database
//!
//! Every profile block has a `{block}.symdb` beside it, and **that object is
//! in no index**. It is named by its block key and by nothing else, so each of
//! the three paths above has to carry it: [`block_deletions`] pairs a block
//! with it on the way out, and [`live_object_keys`] pairs a live block with it
//! so the orphan sweep spares it. A sweep given block keys alone deletes every
//! symbol database in the bucket, and reports nothing: every query still
//! answers, with every frame unnamed.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use krabka_blockstore::{
    BlockDeletion, BlockDeletionReport, BlockMeta, BlockTimestampUnit, CompactionPolicy,
    IndexSnapshotRetain, OrphanSweepStats, ProfileIndex, RetentionWindows, delete_blocks,
    plan_expired_blocks, reconcile_orphans,
};
use krabka_units::Time;
use object_store::ObjectStore;

use crate::{
    compactor::{DownsamplePolicy, compact_once_with_policy},
    error::ProfilesError,
};
mod lifecycle_options;
mod lifecycle_report;
mod run_lifecycle_pass;
mod sweep_orphan_blocks;
pub use lifecycle_options::LifecycleOptions;
pub use lifecycle_report::LifecycleReport;
pub use run_lifecycle_pass::run_lifecycle_pass;
pub use sweep_orphan_blocks::sweep_orphan_blocks;

/// The key of the symbol database that sits beside `block_key`.
///
/// Every profile block has one. The block holds stack-trace ids, and the
/// symbol database is what turns them back into function names, so a block
/// whose symbol database is gone still reads and still answers a query, with
/// every frame unnamed. Nothing but the block key names this object: it is in
/// no index, and no listing tells it apart from a block. Anything that moves
/// or deletes a block therefore has to name it here as well.
#[must_use]
pub fn symdb_key(block_key: &str) -> String {
    format!("{block_key}.symdb")
}

/// Pairs each block key with the sidecar objects that belong to it.
///
/// A profile block has exactly one sidecar, its [`symdb_key`]. Deleting the
/// block alone would leave that object in the bucket forever: the orphan sweep
/// would reclaim it, but only after the block has left the index, and only on
/// a deployment that runs the sweep.
#[must_use]
pub fn block_deletions(block_keys: &[String]) -> Vec<BlockDeletion> {
    block_keys
        .iter()
        .map(|key| BlockDeletion {
            object_key: key.clone(),
            sidecars: vec![symdb_key(key)],
        })
        .collect()
}

/// Every object under the block prefix that the index still needs.
///
/// This is what [`reconcile_orphans`] is given, and it is the whole of what
/// that sweep spares: it deletes every other object under the prefix. **A set
/// built from the block keys alone would delete every symbol database in the
/// bucket**, because a `.symdb` is named by nothing but its block and appears
/// in no index. Nothing would report it. Every query would keep answering, and
/// every frame in every answer would lose its name. Each live block therefore
/// contributes two keys here: its own, and [`symdb_key`] of it.
///
/// [`reconcile_orphans`]: krabka_blockstore::reconcile_orphans
#[must_use]
pub fn live_object_keys(index: &ProfileIndex) -> BTreeSet<String> {
    index
        .compaction_candidates()
        .into_iter()
        .flat_map(|candidate| {
            let sidecar = symdb_key(&candidate.object_key);
            [candidate.object_key, sidecar]
        })
        .collect()
}
