use super::{BlockLevel, Deserialize, Serialize};

/// What compaction knows about a block that its time bounds do not say.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockLineage {
    /// How many rounds of compaction produced the block.
    pub level: BlockLevel,
    /// Rows in the block, as reported by whoever registered it. `0` when the
    /// registering call site did not know, which the planner reads as "not
    /// large enough to seal" rather than as an empty block.
    pub row_count: usize,
    /// The blocks this one replaced, and only the immediate ones. Recording
    /// the transitive ancestry instead would grow without bound as a block
    /// climbs the ladder; the immediate inputs stay bounded by the fan-in and
    /// are what a reader needs to explain where a block came from.
    pub sources: Vec<String>,
}
