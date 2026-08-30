use super::{CompactionCommitError, CompactionOffsetCommitter, WalPosition};

#[derive(Default)]
pub(crate) struct LastCompactedPosition {
    pub(crate) position: Option<WalPosition>,
}

impl CompactionOffsetCommitter for LastCompactedPosition {
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError> {
        self.position = Some(position);
        Ok(())
    }
}
