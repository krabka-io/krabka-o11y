use super::{JobShard, TagScope};

/// A tag-names job for one optional scope over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagNamesJobRequest {
    pub tenant: String,
    pub scope: Option<TagScope>,
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
