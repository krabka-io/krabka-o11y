use super::*;

/// A partition key within a single block's own symbol DB, scoped to that block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct LocalPartition(pub u64);
