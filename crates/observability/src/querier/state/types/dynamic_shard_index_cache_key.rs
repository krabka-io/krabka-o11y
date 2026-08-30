#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DynamicShardIndexCacheKey {
    pub(crate) tenant: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}
