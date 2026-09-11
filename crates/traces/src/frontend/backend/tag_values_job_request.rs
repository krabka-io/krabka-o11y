use super::{JobShard, TenantId};

/// A tag-values job for one tag over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagValuesJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub tag: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}
