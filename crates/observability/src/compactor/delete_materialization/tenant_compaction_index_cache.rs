use super::{BTreeMap, BlockIndex, LabelIndex};

#[derive(Clone, Default)]
pub(crate) struct TenantLogIndexes {
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
}

pub(crate) type TenantCompactionIndexCache = BTreeMap<String, TenantLogIndexes>;
