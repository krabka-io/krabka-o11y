use super::{MetricBlockKind, BlockMeta, CompactionIndexManifest};

/// One persisted metric block and its committed index sidecar description.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactedBlockWrite {
    pub kind: MetricBlockKind,
    pub block_meta: BlockMeta,
    pub manifest: CompactionIndexManifest,
}
