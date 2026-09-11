use super::JobShard;

/// One planned shard, and the querier that will scan it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignedJob {
    /// What to scan.
    pub shard: JobShard,
    /// The `host:port` to scan it on.
    pub querier: String,
}
