use super::*;

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
}
