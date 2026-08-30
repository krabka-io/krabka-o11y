#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum DynamicIndexCacheKey {
    TenantManifest {
        tenant: String,
    },
    TenantShards {
        tenant: String,
        start_ns: i64,
        end_ns: i64,
    },
}
