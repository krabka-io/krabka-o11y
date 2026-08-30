use super::{CompactionCommitError, WalPosition};

pub trait CompactionOffsetCommitter {
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError>;
}
