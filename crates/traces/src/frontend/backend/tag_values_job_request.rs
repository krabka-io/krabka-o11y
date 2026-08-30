use super::*;

/// A tag-values job for one tag over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagValuesJobRequest {
    pub tenant: String,
    pub tag: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
}
