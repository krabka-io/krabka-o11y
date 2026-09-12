use super::BlockMeta;

/// What one compaction pass produced, and what it retired to produce it.
///
/// The two travel together because the caller has to delete the inputs, and it
/// can only do that after the index that no longer names them is durable. A
/// pass that returned its outputs alone would leave the caller no way to name
/// the objects it must now delete, and every pass would double the storage of
/// the range it merged.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactionPassOutcome {
    /// The replacement blocks, one per planned job.
    pub outputs: Vec<BlockMeta>,
    /// The input blocks the index no longer names. They are still in object
    /// storage, and the caller deletes them once the index is saved.
    pub retired_inputs: Vec<String>,
}

impl CompactionPassOutcome {
    /// Whether the pass planned nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty() && self.retired_inputs.is_empty()
    }
}
