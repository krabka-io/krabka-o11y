use super::JobShard;

/// A `TraceQL`-metrics job over a window with a step.
///
/// The job is `/api/metrics/query_range` or `/api/metrics/query`. `instant`
/// selects the instant-query path.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricsJobRequest {
    pub tenant: String,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub instant: bool,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}
