use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TimeRange {
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn new(start_ns: i64, end_ns: i64) -> Result<Self, BlockStoreError> {
        if start_ns > end_ns {
            return Err(BlockStoreError::InvalidTimeRange { start_ns, end_ns });
        }
        Ok(Self { start_ns, end_ns })
    }

    #[must_use]
    pub fn overlaps(self, other: Self) -> bool {
        self.start_ns <= other.end_ns && other.start_ns <= self.end_ns
    }
}
