//! Formally verified pure storage-lifecycle kernels.
//!
//! Runtime crates adapt their domain types at this boundary and delegate the
//! safety-critical arithmetic here. See `docs/verification.md`.

mod compaction;
mod retention;

pub use compaction::compaction_run_ends;
pub use retention::retention_cutoff;
