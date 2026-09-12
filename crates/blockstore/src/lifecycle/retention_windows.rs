use super::Time;

/// How long each tenant's blocks are kept.
///
/// Retention is per tenant and it is configuration, so no signal crate should
/// hold a window of its own. An implementor reads the operator's limits and
/// answers for one tenant at a time. [`plan_expired_blocks`] asks once per
/// tenant it sees, so an implementation that consults a lock or a map is cheap
/// enough.
///
/// A compactor queries a store from several tasks at once, so an implementor
/// is `Send + Sync`.
///
/// [`plan_expired_blocks`]: super::plan_expired_blocks
pub trait RetentionWindows: Send + Sync {
    /// The window a tenant's blocks are kept for. `Time::ZERO` keeps them
    /// forever.
    ///
    /// Zero means "no retention", not "delete everything". That is what
    /// Mimir's `compactor_blocks_retention_period` and Loki's
    /// `retention_period` mean by zero, and what the metrics compaction sweep
    /// already does with it. Reading it the other way round would make an
    /// unconfigured tenant lose every block it has. A negative window is read
    /// the same way as zero.
    fn block_retention(&self, tenant: &str) -> Time;
}
