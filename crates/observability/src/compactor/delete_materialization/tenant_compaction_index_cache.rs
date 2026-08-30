use super::{BTreeMap, BlockIndex, LabelIndex};

pub(crate) type TenantCompactionIndexCache = BTreeMap<String, (LabelIndex, BlockIndex)>;
