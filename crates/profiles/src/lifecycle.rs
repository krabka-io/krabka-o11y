//! What happens to a profile block after it is written.
//!
//! A block is written once and then only ever removed. Three things remove
//! one, and all three run in the compactor pass that
//! [`run_lifecycle_pass`] is:
//!
//! - A merge retires its inputs. The compactor writes one block in their place
//!   and the inputs become unreachable.
//! - Retention expires a block that ends before its tenant's window. See
//!   [`RetentionWindows`](krabka_blockstore::RetentionWindows), which the
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

mod block_deletions;
mod lifecycle_options;
mod lifecycle_report;
mod live_object_keys;
mod run_lifecycle_pass;
mod sweep_orphan_blocks;
mod symdb_key;

pub use block_deletions::block_deletions;
pub use lifecycle_options::LifecycleOptions;
pub use lifecycle_report::LifecycleReport;
pub use live_object_keys::live_object_keys;
pub use run_lifecycle_pass::run_lifecycle_pass;
pub use sweep_orphan_blocks::sweep_orphan_blocks;
pub use symdb_key::symdb_key;
