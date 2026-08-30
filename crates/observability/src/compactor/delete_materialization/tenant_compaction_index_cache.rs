use super::*;

pub(crate) type TenantCompactionIndexCache = BTreeMap<String, (LabelIndex, BlockIndex)>;
