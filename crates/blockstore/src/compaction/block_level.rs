use super::{Deserialize, Display, Formatter, Serialize};

/// How many rounds of compaction produced a block.
///
/// A block a block builder writes straight from ingested data sits at
/// [`BlockLevel::INGESTED`]. Compacting a set of blocks that all sit at level
/// `L` produces one block at level `L + 1`.
///
/// The level is what lets a planner group like with like, and what lets it
/// stop. A [`super::CompactionPolicy`] caps the ladder, and a block at the cap
/// is never an input again, so a block can be rewritten at most `max_level`
/// times however long the compactor runs.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct BlockLevel(pub u32);

impl BlockLevel {
    /// The level of a block written straight from ingested data.
    pub const INGESTED: Self = Self(0);

    /// The level of the block that compacting blocks at this level produces.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// This level as a plain number, for keys and log fields.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Display for BlockLevel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}
