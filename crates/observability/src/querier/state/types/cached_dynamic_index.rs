use super::{Arc, BlockIndex, Instant, LabelIndex};

#[derive(Clone)]
pub(crate) struct CachedDynamicIndex {
    pub(crate) loaded_at: Instant,
    pub(crate) label_index: Arc<LabelIndex>,
    pub(crate) block_index: Arc<BlockIndex>,
}
