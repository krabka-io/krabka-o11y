use super::{CompactionIndexManifest, CompactionRetentionPhase};

/// What one pass of
/// [`compact_metric_blocks_once`](super::compact_metric_blocks_once) did.
///
/// The three parts are in the order the pass did them: it writes the merged
/// blocks and publishes their manifests, it retires the inputs' manifests, and
/// it deletes the block objects that an *earlier* pass retired. The last part
/// belongs to the earlier pass's inputs and not to this one's, which is the
/// point of deferring it. See [`DeferredBlockDeletions`](super::DeferredBlockDeletions).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricCompactionPass {
    /// The manifests of the merged blocks, one per planned job.
    pub outputs: Vec<CompactionIndexManifest>,
    /// What the retirement of the inputs' `.index` manifests did.
    pub manifests_retired: CompactionRetentionPhase,
    /// What the deletion of an earlier pass's retired blocks did.
    pub blocks_deleted: CompactionRetentionPhase,
}

impl MetricCompactionPass {
    /// Whether the pass merged nothing and deleted nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
            && self.manifests_retired.deleted == 0
            && self.blocks_deleted.deleted == 0
    }
}
