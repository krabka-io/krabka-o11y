use super::*;

/// A tag-names job for one optional scope over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagNamesJobRequest {
    pub tenant: String,
    pub scope: Option<TagScope>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
}
