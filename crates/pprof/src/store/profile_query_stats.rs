use std::collections::BTreeSet;

/// Storage metadata used to estimate the cost of a profile query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileQueryStats {
    pub block_count: u64,
    /// Selector-matching public series identities, excluding `__profile_id__`.
    pub fingerprints: BTreeSet<u64>,
    /// Identities in the physical plan, used to detect overlapping replicas.
    pub profiles: BTreeSet<(u64, String, i64)>,
    pub profile_count: u64,
    pub sample_count: u64,
    pub scopes: Vec<ProfileQueryScope>,
    pub deduplication_needed: bool,
}

/// Physical data belonging to one backend component in a query plan.
///
/// Byte counts describe that backend's representation, rather than an estimate
/// of another database's encoding. Parquet footer bytes are indexes; the rest
/// of the file is profile data. Live heads report retained row, label and
/// symbol buffer capacities, excluding allocator metadata and hash buckets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileQueryScope {
    pub component_type: &'static str,
    pub component_count: u64,
    pub block_count: u64,
    pub series_count: u64,
    pub profile_count: u64,
    pub sample_count: u64,
    pub index_bytes: u64,
    pub profile_bytes: u64,
    pub symbol_bytes: u64,
}
