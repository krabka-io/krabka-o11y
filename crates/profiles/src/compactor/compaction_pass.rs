use super::BlockMeta;

/// What one compaction pass wrote, and what it retired to write it.
///
/// The two halves travel together because the retired inputs are only safe to
/// delete once the index that no longer names them is durable. A pass that
/// returned the outputs alone would leave the caller to re-derive the inputs
/// from a plan it no longer holds, and the objects would stay in the bucket
/// for the life of the deployment.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionPass {
    /// The blocks the pass wrote. Each one is already registered in the index.
    pub outputs: Vec<BlockMeta>,
    /// The input blocks the pass replaced. They have left the index, and their
    /// objects and sidecars are still in object storage.
    pub retired_keys: Vec<String>,
}
