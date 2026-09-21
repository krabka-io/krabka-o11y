//! Formally verified pure storage and index kernels.
//!
//! Runtime crates adapt their domain types at this boundary and delegate the
//! safety-critical arithmetic here. See `docs/verification.md`.

mod compaction;
mod overlap;
mod retention;
mod shard_range;
mod sharding;

pub use compaction::compaction_run_ends;
pub use overlap::overlap_window;
pub use retention::retention_cutoff;
pub use shard_range::shard_range;
pub use sharding::bounded_shard_slots;
