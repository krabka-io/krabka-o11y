use super::*;

#[derive(Clone)]
pub(crate) struct CachedDynamicIndex {
    pub(crate) loaded_at: Instant,
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
}
