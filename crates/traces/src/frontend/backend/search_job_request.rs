use super::JobShard;

/// A single search job: a `TraceQL` search over a window, restricted to one
/// shard. That shard is the live hot tier, or one cold block narrowed to a
/// row-group range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchJobRequest {
    pub tenant: String,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub limit: usize,
    pub spss: usize,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}
