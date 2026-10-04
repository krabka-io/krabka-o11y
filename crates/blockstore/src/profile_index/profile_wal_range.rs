use super::{Deserialize, Serialize};

/// Inclusive source offsets represented by a published profile block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProfileWalRange {
    pub partition: i32,
    pub min_offset: i64,
    pub max_offset: i64,
}

impl ProfileWalRange {
    /// Whether this range covers a source record in its tenant's WAL.
    #[must_use]
    pub fn contains(self, partition: i32, offset: i64) -> bool {
        self.partition == partition && self.min_offset <= offset && offset <= self.max_offset
    }
}
