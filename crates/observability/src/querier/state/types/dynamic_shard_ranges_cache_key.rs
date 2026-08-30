use super::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DynamicShardRangesCacheKey {
    pub(crate) tenant: String,
}
